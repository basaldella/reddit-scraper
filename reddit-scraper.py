import argparse
import datetime
import dateutil.parser
import json
import logging
import nltk.tokenize
import os
import praw
import prawcore
import re
import requests
import sys
import time

from multiprocessing import Pool

try:
    import reddit_config as config
except ImportError:
    print("You should create a file named reddit_config.py before running this script.")
    print(
        "Have a look at the README or just rename reddit_config.py and put your Reddit"
    )
    print("API credentials in there.")
    exit(1)

# ~~~ Global constants ~~~ #

# Output encoding
ENCODING = "utf8"
# API endpoint
PUSHSHIFT_ENDPOINT = "https://api.pushshift.io/reddit/search/submission"
# Skip posts deleted by the author?
SKIP_DELETED = config.SKIP_DELETED
# Skip posts deleted by mods/bots?
SKIP_REMOVED = config.SKIP_REMOVED
# Add the username of the users at the beginning of the lines?
PRINT_USERS = config.PRINT_USERS
# Word tokenizer
TOKENIZER = nltk.tokenize.WordPunctTokenizer().tokenize
# Maximum number of retries when the APIs return an error
MAX_RETRIES = 5
# Configuration file field separator
CONFIG_FIELD_SEPARATOR = "\t"
# Default user for posts with no author
DEFAULT_NO_AUTHOR = "[ DELETED_AUTHOR ]"
# Separator for username/post
AUTHOR_SEP = " : "

# Logging
logging.basicConfig(
    format="%(asctime)s\t%(levelname)s\t%(module)s\t%(message)s", level=logging.DEBUG
)

# Disable logging for imported libraries
logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("prawcore").setLevel(logging.INFO)
logging.getLogger("request").setLevel(logging.INFO)


def conflate_spaces(text):
    """
    Conflates space characters into a single space in a string.

    :param text: the string to cleanup.
    :return: the input string, with the space characters conflated.
    """
    return re.sub(r"\s+", " ", text)


def remove_markdown(text):
    """
    Removes markdown formatting from the input text.

    :param text: the text to cleanup
    :return: the cleaned up text.
    """

    # Strip URLs
    text = re.sub(r"\[([^]]+)\][ ]?\(([^)]+)\)", r"\g<1>", text)

    # Remove star-marked bold and italic
    # We apply the regex three times to remove
    # *italic*, **bold**, ***bold and italic***
    text = re.sub(r"\*([^]]+)\*", r"\g<1>", text)
    text = re.sub(r"\*([^]]+)\*", r"\g<1>", text)
    text = re.sub(r"\*([^]]+)\*", r"\g<1>", text)

    # Remove underline-marked bold and italic
    text = re.sub(r"_([^]]+)_", r"\g<1>", text)
    text = re.sub(r"_([^]]+)_", r"\g<1>", text)
    text = re.sub(r"_([^]]+)_", r"\g<1>", text)

    # Remove code
    text = re.sub(r"`([^]]+)`", r"\g<1>", text)

    # Remove strikethrough
    text = re.sub(r"~~([^]]+)~~", r"\g<1>", text)

    # Remove spoilers
    text = re.sub(r">!([^]]+)!<", r"\g<1>", text)

    return text


def scrape_comment_tree(comment):
    """
    Recursively scrapes a comment and all its replies (and each reply's replies, and so on)
    and returns them as a string.

    :param comment: the instance of `CommentForest` to scrape.
    :return: a list containing the comment and all its replies.
    """

    comments = []

    if PRINT_USERS:
        author = comment.author.name if comment.author else DEFAULT_NO_AUTHOR
        comments.extend([author + AUTHOR_SEP + comment.body])
    else:
        comments.extend([comment.body])

    for reply in comment.replies:
        comments.extend(scrape_comment_tree(reply))
    return comments


def scrape_submission(reddit, submission_id, blacklist, output_dir, status_message=""):
    """
    Scrapes a single submission and its comments and appends
    its content into a file.

    :param reddit: the `reddit` instance.
    :param submission_id: the ID of the submission to scrape
    :param status_message: an additional status message (optional)
    :param blacklist: a list of lines to ignore (default `[]`).
    :param output_dir: the output directory
    """

    # Obtain the submission
    submission = reddit.submission(id=submission_id)

    output_file = "{0}{1}{2}.{3}".format(output_dir, os.sep, submission_id, "txt")

    logging.debug(
        'Scraping {0} {1} "{2}"'.format(
            submission.id + " |",
            status_message + " |" if status_message.strip() else "",
            submission.title
            if len(submission.title) < 40
            else submission.title[:37] + "...",
        )
    )

    # Skip deleted (by the author) or removed (due to rule violation) submissions
    if SKIP_DELETED and "[deleted]" in submission.selftext:
        return
    if SKIP_REMOVED and "[removed]" in submission.selftext:
        return

    # Build list of contents
    if PRINT_USERS:
        author = submission.author.name if submission.author else DEFAULT_NO_AUTHOR
        submission_list = [
            author + AUTHOR_SEP + submission.title,
            author + AUTHOR_SEP + submission.selftext,
        ]
    else:
        submission_list = [submission.title, submission.selftext]

    submission.comments.replace_more(None)
    for comment in submission.comments:
        comments = scrape_comment_tree(comment)
        if comments:
            submission_list.extend(comments)

    submission_list = [line.strip() + "\n" for line in submission_list]

    # Put one sentence per line
    sentences = []

    for _submission in submission_list:

        if PRINT_USERS:

            sub_author = _submission[: _submission.index(":") - 1]
            sub_text = _submission[_submission.index(":") + 2 :]

            line_sentences = nltk.tokenize.sent_tokenize(remove_markdown(sub_text))
            clean_lines = [conflate_spaces(sent) for sent in line_sentences]
            clean_lines = [
                sub_author + AUTHOR_SEP + " ".join(TOKENIZER(sent))
                for sent in clean_lines
            ]
        else:
            line_sentences = nltk.tokenize.sent_tokenize(remove_markdown(_submission))
            clean_lines = [conflate_spaces(sent) for sent in line_sentences]
            clean_lines = [" ".join(TOKENIZER(sent)) for sent in clean_lines]

        if len(clean_lines) > 0:
            if len(blacklist) == 0:
                sentences.extend(clean_lines)
            else:
                for line in clean_lines:
                    if line not in blacklist:
                        sentences.append(line)

    # Write to file
    with open(output_file, encoding=ENCODING, mode="w") as f:
        f.writelines(sentence + "\n" for sentence in sentences)


def get_submission_list(start_timestamp, end_timestamp, args=None):
    """
    Scrapes a subreddit for submissions between to given dates. Due to limitations
    of the underlying service, it may not return all the possible submissions, so
    it will be necessary to call this method again. The method requests the results
    in descending orders, so in subsequent calls, you should only update end_timestamp.

    :param start_timestamp: request results after this date/time.
    :param end_timestamp: request results before this date/time.
    :param args: the args to pass to the endpoint
    :return: the JSON object returned by the service.
    """

    # Generic parameters: for each submission we want its ID and timestamp,
    # 500 is the maximum limit, sorted temporally by the most recent
    params = "fields=id,created_utc,subreddit&limit=500&sort=desc&sort_type=created_utc"

    if args:
        for key, value in args.items():
            params += "&{0}={1}".format(key, value)

    url = "{0}?before={1}&after={2}&{3}".format(
        PUSHSHIFT_ENDPOINT, end_timestamp, start_timestamp, params
    )
    resp = requests.get(url)
    return resp.json()


def scrape_all(reddit, start_timestamp, end_timestamp, output_dir, config, blacklist):
    """
    Scrapes the specified subreddit and writes it in an eponymously named
    file in `output_dir`.

    :param reddit: the `reddit` instance.
    :param output_dir: the output directory.
    :param start_timestamp: the starting date of the scraping
    :param end_timestamp: the end date of the scraping
    :param config: the parameters to pass to the Pushshift APIs
    :param blacklist: a list of lines to ignore (default `[]`).
    """

    start_date = datetime.datetime.utcfromtimestamp(start_timestamp).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    end_date = datetime.datetime.utcfromtimestamp(end_timestamp).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    logging.info("Scraping {0} to {1}...".format(start_date, end_date))

    # get_submission_list returns the _most recent_ 500 submissions
    # between the provided timestamps. So, if there are more than 500,
    # we need to iterate.
    smallest_processed_timestamp = end_timestamp

    while smallest_processed_timestamp > start_timestamp:

        submissions = None
        retries = 0
        e = Exception()
        while (not submissions) and (retries < MAX_RETRIES):

            retries += 1

            try:
                submissions = get_submission_list(
                    start_timestamp, smallest_processed_timestamp, args=config
                )
            except json.decoder.JSONDecodeError as _e:
                e = _e
                logging.info(
                    "Failed scraping {0}: tentative {1} of {2}".format(
                        start_date, retries, MAX_RETRIES
                    )
                )

                time.sleep(1)

        if not submissions:
            logging.warning(
                "Error while retrieving {0}: {1}".format(
                    start_date, str(type(e).__name__)
                )
            )
            return

        for submission in submissions["data"]:
            try:

                sub_date = datetime.datetime.utcfromtimestamp(
                    submission["created_utc"]
                ).strftime("%Y-%m-%d")

                sub_output_dir = "{0}{1}{3}{1}{2}".format(
                    output_dir, os.sep, sub_date, submission["subreddit"]
                )
                os.makedirs(sub_output_dir, exist_ok=True)

                scrape_submission(
                    reddit, submission["id"], blacklist, sub_output_dir, sub_date
                )
            except Exception as e:

                logging.warning(
                    "{0}: Failed scraping submission {1} due to {2} {3}".format(
                        start_date,
                        submission["id"],
                        type(e).__name__,
                        ": " + str(e) if str(e) else "",
                    )
                )

        if len(submissions["data"]) == 0:
            break

        smallest_processed_timestamp = submissions["data"][
            len(submissions["data"]) - 1
        ]["created_utc"]

        logging.info(
            "Scraped from {0} to {1}.".format(
                datetime.datetime.utcfromtimestamp(
                    smallest_processed_timestamp
                ).strftime("%Y-%m-%d %H:%M:%S"),
                datetime.datetime.utcfromtimestamp(
                    submissions["data"][0]["created_utc"]
                ).strftime("%Y-%m-%d %H:%M:%S"),
            )
        )

    logging.info("Scraped {0} to {1}.".format(start_date, end_date))


def scrape_subreddit(reddit, sub_id, start_date, end_date, output_dir, blacklist):
    """
    Scrapes the specified subreddit and writes it in an eponymously named
    file in `output_dir`.

    :param reddit: the `reddit` instance.
    :param sub_id: the subreddit to scrape.
    :param output_dir: the output directory.
    :param start_date: the starting date of the scraping, in YYYY-MM-DD format.
    :param end_date: the end date of the scraping, in YYYY-MM-DD format.
    :param blacklist: a list of lines to ignore (default `[]`).
    """

    logging.info("Scraping {0}...".format(sub_id))

    # Create the subreddit's directory
    output_dir = "{0}{1}{2}".format(output_dir, os.sep, sub_id)
    os.makedirs(output_dir, exist_ok=True)

    p1 = dateutil.parser.parse(start_date)
    p2 = dateutil.parser.parse(end_date)

    d1 = datetime.datetime(p1.year, p1.month, p1.day)
    d2 = datetime.datetime(p2.year, p2.month, p2.day, 23, 59)

    start_timestamp = int(d1.timestamp())
    end_timestamp = int(d2.timestamp())

    # get_submission_list returns the _most recent_ 500 submissions
    # between the provided timestamps. So, if there are more than 500,
    # we need to iterate.
    smallest_processed_timestamp = end_timestamp

    while smallest_processed_timestamp > start_timestamp:

        submissions = None
        retries = 0
        e = Exception()
        while (not submissions) and (retries < MAX_RETRIES):

            retries += 1

            try:
                submissions = get_submission_list(
                    start_timestamp,
                    smallest_processed_timestamp,
                    args={"subreddit": sub_id},
                )
            except json.decoder.JSONDecodeError as _e:
                e = _e
                logging.info(
                    "Failed scraping {0}: tentative {1} of {2}".format(
                        sub_id, retries, MAX_RETRIES
                    )
                )

                time.sleep(1)

        if not submissions:
            logging.warning(
                "Error while retrieving {0}: {1}".format(sub_id, str(type(e).__name__))
            )
            return

        for submission in submissions["data"]:
            try:
                sub_date = datetime.datetime.utcfromtimestamp(
                    submission["created_utc"]
                ).strftime("%Y-%m-%d")
                scrape_submission(
                    reddit, submission["id"], blacklist, output_dir, sub_date
                )
            except Exception as e:
                logging.warning(
                    "r/{0}: Failed scraping submission {1} due to {2} {3}".format(
                        sub_id,
                        submission["id"],
                        type(e).__name__,
                        ": " + str(e) if str(e) else "",
                    )
                )

        if len(submissions["data"]) == 0:
            break

        smallest_processed_timestamp = submissions["data"][
            len(submissions["data"]) - 1
        ]["created_utc"]

        logging.info(
            "Scraped {0} from {1} to {2}.".format(
                sub_id,
                datetime.datetime.utcfromtimestamp(
                    smallest_processed_timestamp
                ).strftime("%Y-%m-%d %H:%M:%S"),
                datetime.datetime.utcfromtimestamp(
                    submissions["data"][0]["created_utc"]
                ).strftime("%Y-%m-%d %H:%M:%S"),
            )
        )

    logging.info("Finished scraping {0}.".format(sub_id))


def load_list_from_file(file):
    """
    Loads the lists of subreddits to scrape and returns it. If the input file does
    not exist, prints a message and quits the script.

    :param file: the input file.
    :return: a list of subreddit IDs.
    """

    if os.path.isfile(file):
        with open(file, encoding="utf8") as f:
            lines = f.readlines()
            lines = [s.strip() for s in lines]
            lines = [sub for sub in lines if not sub.startswith("#")]
            logging.debug("First 5 entries: {0}".format(str(lines[:5])))
            return lines
    else:
        print("The file you specified does not exist or it is inaccessible.")
        print("Exiting...")
        exit(1)


def load_blacklist(file):
    """
    Loads the lists of subreddits to scrape and returns it. If the input file does
    not exist, prints a message and quits the script.

    :param file: the input file.
    :return: a list of subreddit IDs.
    """

    if os.path.isfile(file):
        with open(file, encoding="utf8") as f:
            return [l.strip() for l in f.readlines()]
    else:
        print("The blacklist file you specified does not exist or it is inaccessible.")
        print("Exiting...")
        exit(1)


def load_config(file):
    """
    Loads the configuration files for the parameters to pass to Pushshift and returns it. If the input file does
    not exist, prints a message and quits the script.

    :param file: the input file.
    :return: the configuration parameters for Pushshift.
    """

    config = {}

    if os.path.isfile(file):
        with open(file, encoding="utf8") as f:
            for line in f.readlines():
                if not line.startswith("#"):
                    config_entry = line.split(CONFIG_FIELD_SEPARATOR)

                    assert (
                        len(config_entry) == 2
                    ), "Invalid configuration: each line should contain two entries"
                    config[config_entry[0]] = config_entry[1]

        return config

    else:
        print("The config file you specified does not exist or it is inaccessible.")
        print("Exiting...")
        exit(1)


def check_output_directory(output_dir):
    """
    Checks if the output directory exists and is writable. If not, prints a message
    and quits the script.

    :param output_dir: the string representing the output path.
    """

    if not os.access(output_dir, os.W_OK):
        print(
            "Error: {0} is not a valid directory or it is not writable.".format(
                output_dir
            )
        )
        print("Exiting...")
        exit(1)


def do_reddit_login():
    """
    Reads the configuration and and initialises the `reddit` object.

    If someting goes wrong, prints a message and quits the script.

    :return: the `reddit` instance.
    """

    reddit = praw.Reddit(
        client_id=config.CLIENT_ID,
        client_secret=config.CLIENT_SECRET,
        user_agent=config.USER_AGENT,
    )

    # Check if we're actually logged in
    try:
        for _ in reddit.subreddit("test").top("week"):
            break
    except (prawcore.exceptions.OAuthException, prawcore.exceptions.ResponseException):
        print(
            "Login failed. Please double check your username, password, and application tokens."
        )
        exit(1)

    return reddit


def make_splits(start_date, end_date, workers):
    """
    Prepare the script to parallelize the download from the entire Reddit corpus.

    :param start_date: the start date of the scrape
    :param end_date: the end date of the scrape
    :param workers: the number of splits
    :return: the splits, in a list of couples like `[(start_timestamp,end_timestamp) , ... ]`
    """

    p1 = dateutil.parser.parse(start_date)
    p2 = dateutil.parser.parse(end_date)

    d1 = datetime.datetime(p1.year, p1.month, p1.day)
    d2 = datetime.datetime(p2.year, p2.month, p2.day, 23, 59)

    delta = (d2 - d1) / workers

    start_split = d1
    end_split = d1 + delta

    splits = [(int(start_split.timestamp()), int(end_split.timestamp()))]

    while end_split < d2:
        start_split = end_split
        end_split = start_split + delta
        splits.append((int(start_split.timestamp()), int(end_split.timestamp())))

    return splits


def process_all(args):
    """
    Utility function for parallel processing.

    :param args: a tuple containing the args for `scrape_all`.
    """
    # Unpack arguments
    (reddit, start_year, end_year, output_folder, config, blacklist) = args

    scrape_all(reddit, start_year, end_year, output_folder, config, blacklist)


def process_subs(args):
    """
    Utility function for parallel processing.

    :param args: a tuple containing the args for `scrape_subreddit`.
    """
    # Unpack arguments
    (reddit, sub, start_year, end_year, output_folder, blacklist) = args

    scrape_subreddit(reddit, sub, start_year, end_year, output_folder, blacklist)


def process_posts(args):
    """
    Utility function for parallel processing.

    :param args: a tuple containing the args for `scrape_submission`.
    """
    # Unpack arguments
    (reddit, post, blacklist, output_folder) = args

    scrape_submission(reddit, post, blacklist, output_folder)


def main():
    import textwrap

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """\
Scrapes subreddits and puts their content in a plain text file.
Use with --posts to download posts, --subs to download
subreddits, and --config to make custom Pushshift API calls. 
"""
        ),
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)

    mode_group.add_argument(
        "--posts",
        dest="posts_file",
        type=str,
        default="",
        help="A file containing the list of posts to download, one per line.",
    )

    mode_group.add_argument(
        "--subs",
        dest="subs_file",
        type=str,
        # required=False,
        default="",
        help="A file containing the list of subreddits to download, one per line.",
    )

    mode_group.add_argument(
        "--config",
        dest="config_file",
        type=str,
        # required=False,
        default="",
        help="A file containing the arguments for the Pushshift APIs. See config.default.txt for a sample config file.",
    )

    parser.add_argument(
        "--start",
        dest="start_date",
        type=str,
        # required=True,
        help="The date to start parsing from, in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end",
        dest="end_date",
        type=str,
        # required=True,
        help="The final date of the parsing, in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--output",
        dest="output_folder",
        type=str,
        required=True,
        help="The output folder",
    )

    parser.add_argument(
        "--blacklist",
        dest="blacklist_file",
        type=str,
        required=False,
        default="",
        help="A file containing the lines to skip.",
    )
    parser.add_argument(
        "--workers",
        dest="num_workers",
        type=int,
        required=False,
        default=1,
        help="Number of parallel workers",
    )

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()

    args = parser.parse_args()

    if args.config_file or args.subs_file:
        if not (args.start_date and args.end_date):
            parser.error(
                "Start date and end date are required in --config or --subs mode."
            )

        pattern = re.compile("^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$")
        if not (pattern.match(args.start_date) and pattern.match(args.end_date)):
            parser.error("Invalid date format.")

    check_output_directory(args.output_folder)

    try:
        reddit = do_reddit_login()
    except ImportError:
        parser.error("Failed to load configuration. Did you create reddit_config.py?")
        return
        # here return useless since parser.error quits,
        # but necessary to avoid the 'variable might not be initialized' warnings

    if args.subs_file:

        subs = load_list_from_file(args.subs_file)

        blacklist = load_blacklist(args.blacklist_file) if args.blacklist_file else []

        if args.num_workers > 1:
            with Pool(args.num_workers) as p:
                p.map(
                    process_subs,
                    [
                        (
                            reddit,
                            sub,
                            args.start_date,
                            args.end_date,
                            args.output_folder,
                            blacklist,
                        )
                        for sub in subs
                    ],
                )

        else:
            for sub in subs:
                process_subs(
                    (
                        reddit,
                        sub,
                        args.start_date,
                        args.end_date,
                        args.output_folder,
                        blacklist,
                    )
                )

    elif args.posts_file:

        posts = load_list_from_file(args.posts_file)

        blacklist = load_blacklist(args.blacklist_file) if args.blacklist_file else []

        if args.num_workers > 1:
            with Pool(args.num_workers) as p:
                p.map(
                    process_posts,
                    [(reddit, post, blacklist, args.output_folder) for post in posts],
                )

        else:
            for post in posts:
                process_posts((reddit, post, blacklist, args.output_folder))

    else:
        blacklist = load_blacklist(args.blacklist_file) if args.blacklist_file else []
        config = load_config(args.config_file) if args.config_file else {}

        if args.num_workers > 1:
            with Pool(args.num_workers) as p:
                p.map(
                    process_all,
                    [
                        (
                            reddit,
                            start_split,
                            end_split,
                            args.output_folder,
                            config,
                            blacklist,
                        )
                        for (start_split, end_split) in make_splits(
                            args.start_date, args.end_date, args.num_workers
                        )
                    ],
                )

        else:

            start_ts, end_ts = make_splits(args.start_date, args.end_date, 1)[0]
            process_all(
                (reddit, start_ts, end_ts, args.output_folder, config, blacklist)
            )

    print("Done!")
    exit(0)


if __name__ == "__main__":
    main()
