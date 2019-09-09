"""Bulk download utility for the Eurostat dataset."""

import argparse
from pathlib import Path
import time
from typing import List
import sys

from bs4 import BeautifulSoup
import requests

DEFAULT_LIST_URL = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?dir=data&sort=1&sort=2&start=all"
DEFAULT_STAGGER_AMOUNT = 2000


class EurostatDownloader(object):
    def __init__(
        self,
        list_url: str = DEFAULT_LIST_URL,
        stagger_amount: int = DEFAULT_STAGGER_AMOUNT,
        quiet: bool = False,
    ) -> None:
        """Instantiate a new EurostatDownloader for downloading data from list_url.

        :param list_url: URL providing the listing of available datasets
        :param stagger_amount: Minimum amount of milliseconds between two subsequent HTTP requests
        """
        self.list_url = list_url
        self.stagger_amount = stagger_amount
        self.quiet = quiet
        self.last_stagger_finished = 0

    def _print(self, text) -> None:
        """Print to STDOUT unless the quiet flag was passed to the constructor.

        :param text: Text to (maybe) print
        """
        if not self.quiet:
            print(text)

    def _current_time(self) -> int:
        """Returns the system time as milliseconds since epoch."""
        return int(round(time.time() * 1000))

    def _stagger(self) -> None:
        """Ensure that at least stagger_amount milliseconds have elapsed since the previous call to this method."""
        remaining_stagger = (
            self.last_stagger_finished + self.stagger_amount - self._current_time()
        )
        if remaining_stagger > 0:
            time.sleep(remaining_stagger / 1000)
        self.last_stagger_finished = self._current_time()

    def _download_file(self, url: str, path: Path) -> bool:
        """Download file to disk.

        :param url: Url of path on remote server
        :param path: Local path to which file is stored
        """
        self._stagger()
        try:
            response = requests.get(url, allow_redirects=True)
        except Exception as ex:
            print("Failed to download {}: {}".format(url, ex), file=sys.stderr)
            return False
        try:
            path.write_bytes(response.content)
        except Exception as ex:
            print("Failed to store {}: {}".format(path, ex), file=sys.stderr)
            return False
        return True

    def _get_html(self, url: str) -> BeautifulSoup:
        """Get the contents of a URL as HTML.

        :param url: Url to fetch
        :return: Parsed HTML as a BeautifulSoup object.
        """
        self._stagger()
        response = requests.get(url)
        return BeautifulSoup(response.text, "html.parser")

    def _crawl_dataset_urls(self, listing_url: str) -> List[str]:
        """Parse a list of datasets from listing_url.

        :param listing_url: Url to Bulk Download Listing
        :return: List of URL to individual datasets
        """
        self._stagger
        html = self._get_html(listing_url)
        return [a["href"] for a in html.findAll("a", href=True, text="Download")]

    def fetch_all(self, storage_path: Path) -> None:
        """Download and store on disk all data tables from the Bulk Download Listing.

        :param storage_path: Directory on disk to which store the data
        """
        dataset_urls = self._crawl_dataset_urls(self.list_url)
        total_count = len(dataset_urls)
        self._print(
            "Found total {} datasets, downloading with {} millisecond staggering".format(
                total_count, self.stagger_amount
            )
        )
        for idx, dataset_url in enumerate(dataset_urls):
            filename = dataset_url.split("downfile=data%2F")[-1]
            file = storage_path / filename
            if file.exists():
                self._print(
                    "[{}/{}] SKIP: already found {} at {}".format(
                        idx + 1, total_count, filename, file
                    )
                )
                continue
            if self._download_file(dataset_url, file):
                self._print(
                    "[{}/{}] SUCCESS: stored {} to {}".format(
                        idx + 1, total_count, filename, file
                    )
                )
            else:
                self._print(
                    "[{}/{}] FAILURE: unable to download {}".format(
                        idx + 1, total_count, dataset_url
                    )
                )


if __name__ == "__main__":

    def dir_path(path: str) -> Path:
        path = Path(path)
        if path.is_dir():
            return path
        raise NotADirectoryError(path)

    parser = argparse.ArgumentParser(
        description="Fetch a copy of the Eurostat dataset."
    )
    parser.add_argument(
        "-l",
        "--listing_url",
        type=str,
        default=DEFAULT_LIST_URL,
        help="Link to the Bulk Download Listing's /data directory.",
    )
    parser.add_argument(
        "-s",
        "--stagger",
        type=int,
        default=DEFAULT_STAGGER_AMOUNT,
        help="The minimum amount of time (in milliseconds) that needs to pass between two subsequent HTTP requests.",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Only output errors to the STDERR stream.",
    )
    parser.add_argument(
        "output_dir",
        type=dir_path,
        help="Local file system target to store the data in.",
    )
    args = parser.parse_args()

    EurostatDownloader(args.listing_url, args.stagger, args.quiet).fetch_all(
        Path(args.output_dir)
    )
