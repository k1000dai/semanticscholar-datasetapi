import requests
from typing import Dict, Any, List, Optional
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SemanticScholarDataset:
    """
    A Python wrapper for the Semantic Scholar Dataset API.

    Documentation: https://api.semanticscholar.org/api-docs/datasets
    """

    # API Configuration
    BASE_URL = "https://api.semanticscholar.org/datasets/v1"
    AVAILABLE_DATASETS = [
        "abstracts",
        "authors",
        "citations",
        "embeddings-specter_v1",
        "embeddings-specter_v2",
        "paper-ids",
        "papers",
        "publication-venues",
        "s2orc",
        "tldrs",
    ]

    # Request Configuration
    RETRY_TOTAL = 5
    RETRY_BACKOFF_FACTOR = 0.3
    RETRY_STATUS_FORCELIST = [429, 500, 502, 503, 504]
    CHUNK_SIZE = 1024 * 64
    REQUEST_TIMEOUT = 10
    SAVE_DIR = "."  # Default save path

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        if self.api_key is not None:
            self.headers = {"x-api-key": self.api_key}
        else:
            self.headers = {}

    def __create_session(self) -> requests.Session:
        """Create a configured session with retry logic."""
        session = requests.Session()
        retry = Retry(
            total=self.RETRY_TOTAL,
            backoff_factor=self.RETRY_BACKOFF_FACTOR,
            status_forcelist=self.RETRY_STATUS_FORCELIST,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def __set_save_path(self, save_path: str) -> None:
        """Set the save path for downloaded files."""
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        self.SAVE_DIR = save_path

    def __validate_dataset(self, datasetname: str) -> None:
        """Validate dataset name."""
        if datasetname not in self.AVAILABLE_DATASETS:
            raise ValueError(
                f"Invalid dataset name '{datasetname}'. Available datasets: {self.AVAILABLE_DATASETS}"
            )

    def __validate_api_key(self) -> None:
        """Validate API key presence."""
        if self.api_key is None:
            raise ValueError(
                "API key is required to access the Semantic Scholar Dataset API."
            )

    def __download_file(self, url: str, save_name: str) -> None:
        """Download a file from URL and save it locally."""
        session = self.__create_session()
        try:
            with session.get(
                url, headers=self.headers, stream=True, timeout=self.REQUEST_TIMEOUT
            ) as r:
                r.raise_for_status()
                with open(save_name, "wb") as f:
                    for chunk in r.iter_content(chunk_size=self.CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
            logger.info(f"Downloaded {save_name}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {save_name}: {str(e)}")
            raise

    def __api_request(self, url: str) -> Dict[str, Any]:
        """Make an API request and return JSON response."""
        session = self.__create_session()
        try:
            response = session.get(
                url, headers=self.headers, timeout=self.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {url}: {str(e)}")
            raise

    def get_available_releases(self) -> list:
        url = f"{self.BASE_URL}/release"
        response = self.__api_request(url)
        return response

    def get_available_datasets(self) -> list:
        return self.AVAILABLE_DATASETS

    def get_download_urls_from_release(
        self, datasetname: Optional[str] = None, release_id: str = "latest"
    ) -> Dict[str, Any]:
        """Get download URLs for a specific release."""
        self.__validate_api_key()
        self.__validate_dataset(datasetname)

        url = f"{self.BASE_URL}/release/{release_id}/dataset/{datasetname}"

        response = self.__api_request(url)
        return response

    def get_download_urls_from_diffs(
        self,
        start_release_id: Optional[str] = None,
        end_release_id: str = "latest",
        datasetname: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get download URLs for diffs between releases."""
        self.__validate_api_key()
        self.__validate_dataset(datasetname)

        url = f"{self.BASE_URL}/diffs/{start_release_id}/to/{end_release_id}/{datasetname}"
        logger.info(
            f"Getting diffs from release {start_release_id} to release {end_release_id}..."
        )
        response = self.__api_request(url)
        return response

    def download_latest_release(
        self, datasetname: Optional[str] = None, save_dir=None
    ) -> None:
        """Download the latest release of a dataset."""
        self.__validate_dataset(datasetname)
        response = self.get_download_urls_from_release(datasetname)
        download_urls = response.get("files", [])

        if not download_urls:
            raise ValueError(f"No download URLs found for dataset '{datasetname}'.")

        logger.info("Getting latest release...")
        logger.info("Found {} download URLs.".format(len(download_urls)))

        if save_dir:
            self.__set_save_path(save_dir)

        for i, url in enumerate(download_urls):
            save_name = os.path.join(self.SAVE_DIR, f"{datasetname}_latest_{i}.json.gz")
            self.__download_file(url, save_name)

        logger.info("Download complete.")

    def download_past_release(
        self, release_id: str, datasetname: Optional[str] = None, save_dir=None
    ) -> None:
        """Download a past release of a dataset."""
        self.__validate_dataset(datasetname)

        if release_id == "latest":
            raise ValueError("Please provide a specific release ID.")

        available_releases = self.get_available_releases()
        if release_id not in available_releases:
            raise ValueError(
                f"Invalid release ID '{release_id}'. Available releases: {available_releases}"
            )

        response = self.get_download_urls_from_release(datasetname, release_id)
        download_urls = response.get("files", [])

        if not download_urls:
            raise ValueError(
                f"No download URLs found for dataset '{datasetname}' release '{release_id}'."
            )

        logger.info(f"Getting release {release_id}...")
        logger.info("Found {} download URLs.".format(len(download_urls)))

        if save_dir:
            self.__set_save_path(save_dir)

        for i, url in enumerate(download_urls):
            save_name = os.path.join(self.SAVE_DIR, f"{datasetname}_{release_id}_{i}.json.gz")
            self.__download_file(url, save_name)

    def download_diffs(
        self,
        start_release_id: str,
        end_release_id: str,
        datasetname: Optional[str] = None,
        save_dir=None,
    ) -> None:
        """Download diffs between two releases of a dataset."""
        self.__validate_dataset(datasetname)

        response = self.get_download_urls_from_diffs(
            start_release_id, end_release_id, datasetname
        )
        diffs = response.get("diffs", [])

        if save_dir:
            self.__set_save_path(save_dir)

        for diff in diffs:
            from_release = diff.get("from_release")
            to_release = diff.get("to_release")
            update_files = diff.get("update_files", [])
            delete_files = diff.get("delete_files", [])
            for i, url in enumerate(update_files):
                save_name = (
                    f"{datasetname}_{from_release}_{to_release}_update_{i}.json.gz"
                )
                save_name = os.path.join(self.SAVE_DIR, save_name)
                self.__download_file(url, save_name)
            for i, url in enumerate(delete_files):
                save_name = (
                    f"{datasetname}_{from_release}_{to_release}_delete_{i}.json.gz"
                )
                save_name = os.path.join(self.SAVE_DIR, save_name)
                self.__download_file(url, save_name)

        logger.info("Download complete.")
