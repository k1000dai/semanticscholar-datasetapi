import requests
from typing import Dict, Any
import logging

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
    
    def get_available_releases(self) -> list:
        url = f"{self.BASE_URL}/release"
        response = requests.get(url)
        return response.json()

    def get_available_datasets(self) -> list:
        return self.AVAILABLE_DATASETS
    
    def get_download_urls_from_release(self, datasetname:str = None,release_id: str = "latest") -> Dict[str, Any]:
        if self.api_key is None:
            raise ValueError("API key is required to access the Semantic Scholar Dataset API.")
        
        if datasetname not in self.AVAILABLE_DATASETS:
            raise ValueError(f"Invalid dataset name. Available datasets: {self.AVAILABLE_DATASETS}")
        
        url = f"{self.BASE_URL}/release/{release_id}/dataset/{datasetname}"

        response = requests.get(url, headers=self.headers)
        return response.json()

    def download_latest_release(self, datasetname:str = None):
        if datasetname not in self.AVAILABLE_DATASETS:
            raise ValueError(f"Invalid dataset name. Available datasets: {self.AVAILABLE_DATASETS}")
        
        response = self.get_download_urls_from_release(datasetname)
        download_urls = response.get("files", [])
        
        if download_urls == []:
            raise ValueError("No download URLs found.")
        
        logger.info("Getting latest release...")
        logger.info("Found {} download URLs.".format(len(download_urls)))
        
        for i, url in enumerate(download_urls):
            with requests.get(url, headers=self.headers, stream=True, timeout=30) as r:
                r.raise_for_status()
                save_name = f"{datasetname}_latest_release_{i}.json.gz"
                with open(save_name, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*64):
                        if chunk:
                            f.write(chunk)
                
                logger.info(f"Downloaded {save_name}")
    
    def download_past_release(self, release_id: str, datasetname:str = None):
        if datasetname not in self.AVAILABLE_DATASETS:
            raise ValueError(f"Invalid dataset name. Available datasets: {self.AVAILABLE_DATASETS}")
    
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
            with requests.get(url, headers=self.headers, stream=True, timeout=30) as r:
                r.raise_for_status()
                save_name = f"{datasetname}_release_{release_id}_{i}.json.gz"
                with open(save_name, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*64):
                        if chunk:
                            f.write(chunk)
                
                logger.info(f"Downloaded {save_name}")
    
    
            
                
                    