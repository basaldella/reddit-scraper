# Reddit Scraper

Scrapes subreddits and puts their content in a file. 

Just run

```python reddit-scraper.py```

To see the parameters required by the script.

## Example 

To scrape 
* the first two days of 2018
* of the subs defined in `subreddits.txt`
* saving the content in `scraped`
* using 8 parallel workers
* and ignoring the sentences defined in `blacklist.txt`,

you should run:
```python reddit-scraper.py --subs subreddits.txt --output scraped --start 2018-01-01 --end 2018-01-02 --workers 8 --blacklist blacklist.txt```