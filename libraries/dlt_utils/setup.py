# setup.py

from setuptools import setup, find_packages

setup(
    name="openalex-dlt-utils",  # The name of your package (pip installable name)
    version="0.1.5",            # Your package version
    author="Artem Kazmerchuk/OurResearch",
    description="Utility functions for OpenAlex DLT pipelines",
    packages=find_packages(),   # This will find your 'utils' package and any sub-packages
                                # It looks for directories with __init__.py
    install_requires=[
        "nameparser>=1.1.3",           
        "pandas>=1.5.3",         # Required by Pandas UDFs implicitly
        "databricks-sdk>=0.20.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8'
)