#!/usr/bin/env python3
"""
Example usage of the Semantic Scholar Dataset API wrapper.
This script demonstrates various capabilities of the API client.
"""

import os
import logging
from typing import Optional
from semanticscholar_datasetapi import SemanticScholarDataset

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_api_key() -> Optional[str]:
    """Get API key from environment variable."""
    api_key = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
    if not api_key:
        logger.warning("No API key found in environment variables. Some features may be limited.")
    return api_key

def demonstrate_dataset_info(client: SemanticScholarDataset) -> None:
    """Demonstrate basic dataset information retrieval."""
    logger.info("Available datasets:")
    available_datasets = client.get_available_datasets()
    for dataset in available_datasets:
        logger.info(f"- {dataset}")

    logger.info("\nAvailable releases:")
    releases = client.get_available_releases()
    for release in releases:
        logger.info(f"- {release}")

def demonstrate_diff_operations(client: SemanticScholarDataset) -> None:
    """Demonstrate diff operations between releases."""
    try:
        # Example: Get diff URLs between a specific release and latest
        dataset_name = "papers"
        start_release = "2024-12-31"
        logger.info(f"\nGetting diffs for {dataset_name} from {start_release} to latest...")
        
        diff_urls = client.get_download_urls_from_diffs(
            start_release_id=start_release,
            end_release_id="latest",
            datasetname=dataset_name
        )
        logger.info(f"Found diff information: {diff_urls}")

        # Download the diffs
        logger.info("\nDownloading diffs...")
        client.download_diffs(
            start_release_id=start_release,
            end_release_id="latest",
            datasetname=dataset_name
        )
    except ValueError as e:
        logger.error(f"Error in diff operations: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in diff operations: {str(e)}")

def main() -> None:
    """Main execution function."""
    try:
        # Initialize client with API key from environment
        api_key = get_api_key()
        client = SemanticScholarDataset(api_key=api_key)

        # Demonstrate different API capabilities
        demonstrate_dataset_info(client)
        demonstrate_diff_operations(client)

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
