from setuptools import setup, find_packages

setup(
    name="semanticscholar_datasetapi",
    version="0.1.2",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
        "typing;python_version<'3.8'",  # For Python 3.7 compatibility
    ],
    author="Kohei Sendai",
    author_email="your.email@example.com",  # Update with your email
    description="A Python wrapper for the Semantic Scholar Dataset API that provides easy access to academic papers, citations, and related data",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/k1000dai/semanticscholar-datasetapi",
    project_urls={
        "Bug Tracker": "https://github.com/k1000dai/semanticscholar-datasetapi/issues",
        "Documentation": "https://github.com/k1000dai/semanticscholar-datasetapi#readme",
        "Source Code": "https://github.com/k1000dai/semanticscholar-datasetapi",
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Typing :: Typed",
    ],
    keywords="semantic scholar, dataset, academic papers, citations, research, api",
    python_requires=">=3.7",
)
