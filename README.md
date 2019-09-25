# Reddit Scraper

A Python Reddit scraper based on [Praw](https://praw.readthedocs.io/en/latest/) and [Pushshift](https://pushshift.io). 

This script allows you to:
- Download a list of posts;
- Download a list of subreddits;
- Make arbitrary [API calls to Pushshift](https://pushshift.io/api-parameters/) to build more refined datasets.

The usage should be pretty-explanatory. The only think you should know is that 
you will need to get [an API Key](https://www.reddit.com/prefs/apps) from Reddit, copy it
in `reddit_config.sample.py`, and rename the file to `reddit_config.py`.

```
usage: reddit-scraper.py [-h]
                         (--subs SUBS_FILE | --posts POSTS_FILE | --config CONFIG_FILE)
                         --output OUTPUT_FOLDER --start START_DATE --end
                         END_DATE [--blacklist BLACKLIST_FILE]
                         [--workers NUM_WORKERS]

Scrapes subreddits and puts their content in a plain text file.
The scraping is date-based: the script starts from START_DATE and 
ends at END_DATE.

optional arguments:
  -h, --help            show this help message and exit
  --subs SUBS_FILE      A file containing the list of subreddits to scrape,
                        one per line.
  --posts POSTS_FILE    A file containing the list of posts to scrape, one per
                        line.
  --config CONFIG_FILE  A file containing the arguments for the Pushshift
                        APIs.
  --output OUTPUT_FOLDER
                        The output folder
  --start START_DATE    The date to start parsing from, in YYYY-MM-DD format
  --end END_DATE        The final date of the parsing, in YYYY-MM-DD format
  --blacklist BLACKLIST_FILE
                        A file containing the lines to skip.
  --workers NUM_WORKERS
                        Number of parallel scraper workers
```

## Examples:

1. Download all posts in the subreddits specified in `subreddits.txt`, from January 1, 2015 
to December 31, 2016, using 8 parallel processes, save them in `scraped/`, and ignoring the lines 
defined in `blacklist.txt`:

  `python reddit-scraper.py --subs subreddits.txt --output scraped --start 2015-01-01 --end 2016-12-31 --workers 8  --blacklist blacklist.txt`

2. Download the post specified in `posts.txt`, and save them in `scraped/`:

`python reddit-scraper.py --posts posts.txt --output scraped`
  
3. Use the [Pushshift API](https://pushshift.io/api-parameters/) to look for posts in Reddit, 
using the parameters provided in `config.default.txt`from January 1, 2019 
to January 2, 2019, using 8 parallel processes, and save them in `scraped/`:
`python reddit-scraper.py --config config.default.txt --output scraped --start 2019-01-01 --end 2019-01-02 --workers 8`
