# Eurostat Downloader

Downloads a copy of the Eurostat dataset to disk. 

## Requirements
 * Python 3
 * requests
 * BeautifulSoup 4

## Usage
```
usage: eurostat_downloader.py [-h] [-l LISTING_URL] [-s STAGGER] [-q]
                              output_dir

Fetch a copy of the Eurostat dataset.

positional arguments:
  output_dir            Local file system target to store the data in.

optional arguments:
  -h, --help            show this help message and exit
  -l LISTING_URL, --listing_url LISTING_URL
                        Link to the Bulk Download Listing's /data directory.
  -s STAGGER, --stagger STAGGER
                        The minimum amount of time (in milliseconds) that
                        needs to pass between two subsequent HTTP requests.
  -q, --quiet           Only output errors to the STDERR stream.
```
