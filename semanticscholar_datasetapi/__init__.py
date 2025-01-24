"""
Semantic Scholar Dataset API Wrapper

A Python library for interacting with the Semantic Scholar Dataset API,
providing access to academic papers, citations, and research data.

Example:
    >>> from semanticscholar_datasetapi import SemanticScholarDataset
    >>> client = SemanticScholarDataset(api_key="your-api-key")
    >>> datasets = client.get_available_datasets()
"""

from .api import SemanticScholarDataset

__version__ = "0.1.0"
__author__ = "Kohei Sendai"
__license__ = "MIT"
__copyright__ = "Copyright 2025 Kohei Sendai"

__all__ = ["SemanticScholarDataset"]

# API Documentation URL
__docs__ = "https://api.semanticscholar.org/api-docs/datasets"
