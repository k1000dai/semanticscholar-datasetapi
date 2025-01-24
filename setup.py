from setuptools import setup, find_packages

setup(
    name="semanticscholar_datasetapi",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
    ],
    author="Kohei Sendai",
    description="Python wrapper for the Semantic Scholar Dataset API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/k1000dai/semanticscholar-datasetapi",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
