import requests
from typing import Dict, Any
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SemanticScholarDataset:
    """
    A Python wrapper for the Semantic Scholar Dataset API.

    Documentation: https://api.semanticscholar.org/api-docs/datasets
    """

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

    def __init__(self, api_key: str = None):
        self.api_key = api_key
        if self.api_key is not None:
            self.headers = {"x-api-key": self.api_key}

    def __download_file(self, url: str, save_name: str):
        session = requests.Session()
        retry = Retry(
            total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        with session.get(url, headers=self.headers, stream=True, timeout=10) as r:
            r.raise_for_status()
            with open(save_name, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)

        logger.info(f"Downloaded {save_name}")

    def __api_request(self, url: str):
        session = requests.Session()
        retry = Retry(
            total=5, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        response = session.get(url, headers=self.headers)
        response.raise_for_status()  # Ensure we raise an error for bad responses
        return response.json()

    def get_available_releases(self) -> list:
        url = f"{self.BASE_URL}/release"
        response = self.__api_request(url)
        return response

    def get_available_datasets(self) -> list:
        return self.AVAILABLE_DATASETS

    def get_download_urls_from_release(
        self, datasetname: str = None, release_id: str = "latest"
    ) -> Dict[str, Any]:
        if self.api_key is None:
            raise ValueError(
                "API key is required to access the Semantic Scholar Dataset API."
            )

        if datasetname not in self.AVAILABLE_DATASETS:
            raise ValueError(
                f"Invalid dataset name. Available datasets: {self.AVAILABLE_DATASETS}"
            )

        url = f"{self.BASE_URL}/release/{release_id}/dataset/{datasetname}"

        response = self.__api_request(url)
        return response

    def get_download_urls_from_diffs(
        self, start_release_id: str = None, end_release_id="latest", datasetname=None
    ) -> Dict[str, Any]:
        if self.api_key is None:
            raise ValueError(
                "API key is required to access the Semantic Scholar Dataset API."
            )

        if datasetname not in self.AVAILABLE_DATASETS:
            raise ValueError(
                f"Invalid dataset name. Available datasets: {self.AVAILABLE_DATASETS}"
            )

        url = f"{self.BASE_URL}/diffs/{start_release_id}/to/{end_release_id}/{datasetname}"
        logger.info(
            f"Getting diffs from release {start_release_id} to release {end_release_id}..."
        )
        response = self.__api_request(url)
        return response

    def download_latest_release(self, datasetname: str = None):
        if datasetname not in self.AVAILABLE_DATASETS:
            raise ValueError(
                f"Invalid dataset name. Available datasets: {self.AVAILABLE_DATASETS}"
            )

        response = self.get_download_urls_from_release(datasetname)
        download_urls = response.get("files", [])

        if download_urls == []:
            raise ValueError("No download URLs found.")

        logger.info("Getting latest release...")
        logger.info("Found {} download URLs.".format(len(download_urls)))

        for i, url in enumerate(download_urls):
            save_name = f"{datasetname}_latest_{i}.json.gz"
            self.__download_file(url, save_name)

        logger.info("Download complete.")

    def download_past_release(self, release_id: str, datasetname: str = None):
        if datasetname not in self.AVAILABLE_DATASETS:
            raise ValueError(
                f"Invalid dataset name. Available datasets: {self.AVAILABLE_DATASETS}"
            )

        if release_id == "latest":
            raise ValueError("Please provide a specific release ID.")

        if release_id not in self.get_available_releases():
            raise ValueError("Invalid release ID.")

        response = self.get_download_urls_from_release(datasetname, release_id)
        download_urls = response.get("files", [])

        if download_urls == []:
            raise ValueError("No download URLs found.")

        logger.info(f"Getting release {release_id}...")
        logger.info("Found {} download URLs.".format(len(download_urls)))

        for i, url in enumerate(download_urls):
            save_name = f"{datasetname}_{release_id}_{i}.json.gz"
            self.__download_file(url, save_name)

    def download_diffs(
        self, start_release_id: str, end_release_id: str, datasetname: str = None
    ):
        if datasetname not in self.AVAILABLE_DATASETS:
            raise ValueError(
                f"Invalid dataset name. Available datasets: {self.AVAILABLE_DATASETS}"
            )

        response = self.get_download_urls_from_diffs(
            start_release_id, end_release_id, datasetname
        )
        diffs = response.get("diffs", [])

        for diff in diffs:
            from_release = diff.get("from_release")
            to_release = diff.get("to_release")
            update_files = diff.get("update_files", [])
            delete_files = diff.get("delete_files", [])
            for i, url in enumerate(update_files):
                save_name = (
                    f"{datasetname}_{from_release}_{to_release}_update_{i}.json.gz"
                )
                self.__download_file(url, save_name)
            for i, url in enumerate(delete_files):
                save_name = (
                    f"{datasetname}_{from_release}_{to_release}_delete_{i}.json.gz"
                )
                self.__download_file(url, save_name)

        logger.info("Download complete.")
