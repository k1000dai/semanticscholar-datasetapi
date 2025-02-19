# Semantic Scholar Dataset API Wrapper

A Python wrapper for the Semantic Scholar Dataset API that provides easy access to academic papers, citations, and related data.

## Description

This library provides a simple interface to interact with the Semantic Scholar Dataset API, allowing you to:
- Access various academic datasets (papers, citations, authors, etc.)
- Download dataset releases
- Get diffs between releases
- Manage large dataset downloads efficiently

## Installation

```bash
pip install semanticscholar-datasetapi
```

## Requirements

- Python 3.7+
- requests

## Basic Usage

```python
from semanticscholar_datasetapi import SemanticScholarDataset
import os

# Initialize the client with your API key
api_key = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
client = SemanticScholarDataset(api_key=api_key)

# List available datasets
datasets = client.get_available_datasets()
print(datasets)

# Get latest release information
releases = client.get_available_releases()
print(releases)

# Download latest release of a specific dataset
client.download_latest_release(datasetname="papers", save_dir="downloads")

# Get diffs between releases
client.download_diffs(
    start_release_id="2024-12-31",
    end_release_id="latest",
    datasetname="papers",
    save_dir="diffs"
)
```

## Available Datasets

The API provides access to the following datasets:
- abstracts
- authors
- citations
- embeddings-specter_v1
- embeddings-specter_v2
- paper-ids
- papers
- publication-venues
- s2orc
- tldrs

## API Reference

### Main Methods

#### `SemanticScholarDataset(api_key: Optional[str] = None)`
Initialize the API client with an optional API key.

- `api_key`: API key for accessing the Semantic Scholar Dataset API. Required for most operations.

#### `get_available_releases() -> list`
Get a list of all available dataset releases.

#### `get_available_datasets() -> list`
Get a list of all available datasets.

#### `get_download_urls_from_release(datasetname: Optional[str] = None, release_id: str = "latest") -> Dict[str, Any]`
Get download URLs for a specific release of a dataset.

- `datasetname`: Name of the dataset to get URLs for
- `release_id`: ID of the release (defaults to "latest")

#### `get_download_urls_from_diffs(start_release_id: Optional[str], end_release_id: str = "latest", datasetname: Optional[str]) -> Dict[str, Any]`
Get download URLs for differences between two releases.

- `start_release_id`: Starting release ID
- `end_release_id`: Ending release ID (defaults to "latest")
- `datasetname`: Name of the dataset to get diff URLs for

#### `download_latest_release(datasetname: Optional[str] = None, save_dir: Optional[str] = None, range: Optional[range] = None) -> None`
Download the latest release of a specific dataset.

- `datasetname`: Name of the dataset to download
- `save_dir`: Directory to save downloaded files (defaults to current directory)
- `download_range`: Optional range of indices to download from the list of files

#### `download_past_release(release_id: str, datasetname: Optional[str] = None, save_dir: Optional[str] = None, range: Optional[range] = None) -> None`
Download a specific past release of a dataset.

- `release_id`: ID of the release to download
- `datasetname`: Name of the dataset to download
- `save_dir`: Directory to save downloaded files (defaults to current directory)
- `download_range`: Optional range of indices to download from the list of files

#### `download_diffs(start_release_id: str, end_release_id: str, datasetname: Optional[str] = None, save_dir: Optional[str] = None) -> None`
Download the differences between two releases of a dataset.

- `start_release_id`: Starting release ID
- `end_release_id`: Ending release ID
- `datasetname`: Name of the dataset to download diffs for
- `save_dir`: Directory to save downloaded files (defaults to current directory)

### Error Handling

The library includes comprehensive error handling for:
- Invalid dataset names
- Missing API keys
- Network errors
- Invalid release IDs

### File Naming

Downloaded files follow these naming patterns:
- Latest release: `{datasetname}_latest_{index}.json.gz`
- Past release: `{datasetname}_{release_id}_{index}.json.gz`
- Diffs: 
  - Updates: `{datasetname}_{from_release}_{to_release}_update_{index}.json.gz`
  - Deletes: `{datasetname}_{from_release}_{to_release}_delete_{index}.json.gz`

## Environment Variables

- `SEMANTIC_SCHOLAR_API_KEY`: Your API key for the Semantic Scholar Dataset API

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- Semantic Scholar for providing the Dataset API
- The academic community for maintaining and contributing to the datasets
