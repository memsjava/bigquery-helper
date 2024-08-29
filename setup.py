from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="bigquery-helper",
    version="0.1.0",
    author="Rabemampiandra",
    author_email="memsjava@gmail.com",
    description="A helper package for Google BigQuery operations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/memsjava/bigquery-helper",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
    install_requires=[
        "google-cloud-bigquery>=3.0.0",
        "pandas>=1.0.0",
        "pandas-gbq>=0.14.0",
    ],
)