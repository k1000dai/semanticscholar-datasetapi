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
client.download_latest_release(datasetname="papers")

# Get diffs between releases
client.download_diffs(
    start_release_id="2024-12-31",
    end_release_id="latest",
    datasetname="papers"
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

#### `get_available_releases() -> list`
Get a list of all available dataset releases.

#### `get_available_datasets() -> list`
Get a list of all available datasets.

#### `download_latest_release(datasetname: Optional[str] = None) -> None`
Download the latest release of a specific dataset.

#### `download_past_release(release_id: str, datasetname: Optional[str] = None) -> None`
Download a specific past release of a dataset.

#### `download_diffs(start_release_id: str, end_release_id: str, datasetname: Optional[str] = None) -> None`
Download the differences between two releases of a dataset.

### Error Handling

The library includes comprehensive error handling for:
- Invalid dataset names
- Missing API keys
- Network errors
- Invalid release IDs

## Environment Variables

- `SEMANTIC_SCHOLAR_API_KEY`: Your API key for the Semantic Scholar Dataset API

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- Semantic Scholar for providing the Dataset API
- The academic community for maintaining and contributing to the datasets
