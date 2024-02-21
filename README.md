# Project README

## Earthquake Data Layer

Welcome to the Earthquake Data Layer! This project is designed to collect earthquake data from an API,
update the dataset periodically and handle storage operations.<br>
The goal is to build a machine learning model that predicts earthquakes weekly and provides insights into seismic activities.

## Table of Contents

1. [Responsibilities](#responsibilities)
   - [Dataset Collection](#dataset-collection)
   - [Dataset Update](#dataset-update)
   - [Storage Operations](#storage-operations)
2. [Getting Started](#getting-started)
   - [Prerequisites](#prerequisites)
   - [Installation](#installation)
3. [Usage](#usage)
4. [Contributing](#contributing)
5. [License](#license)


## Responsibilities

### Dataset Collection and Update

The aim of this project is to collect a dataset of earthquakes via an API (https://earthquake.usgs.gov/fdsnws/event/1/query) and update it periodically.
When the application is initiated all the data since {setting.EARLIEST_EARTHQUAKE_DATE} will be collected. In case of
error the system is designed to attempt to complete the collection process (patch) after settings.COLLECTION_SLEEP_TIME seconds.
To assist in the patching and to prevent rerunning the long collection process, a json file is stored on cloud, its
location is marked in the below storage scheme with [0].
The data collection process was tested with run time of approximately 40 minutes per decade of data. <br>
An update of the last 12 months is initiated with a call to the route defined @/update/{date}.
For both dataset collection and update the data retrieved from the API is batched (query, process and save) in regard
to its calendar month, the results of each of these batches is stored on cloud at [1].
The data collection process was tested with run time of approximately 40 minutes per decade of data.

### Storage Operations

The system includes functionality for downloading and uploading data to remote storage.<br>
Data will be uploaded to a dedicated bucket in the cloud, using the following the scheme:
```
{bucket_root}
    /data
        [0] collection_metadata.json
        [1] batch_metadata.parquet: contains the metadata from each batch (each row is a bacth)
        / raw_data
            / {year}
                [2] {year}_{month}_raw_data.parquet: contains each month/batch's data (each row is an earthquake)
                .
                .
                .
```

## Getting Started

### Prerequisites

- poetry (https://python-poetry.org/docs/)
- An S3 compatible cloud storage account. Check out https://notes.amarvyas.in/list-s3-compatible-storage-providers/ for suggestions

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yonatan1988eisenberg/earthquake_data_layer
   cd earthquake_data_layer

2. Install dependencies:
    ```bash
   poetry install
   poetry shell

3. Configure storage settings in the configuration file .env.
In addition, its worth taking a look setting and definitions to see that the chosen values work for you.

### Usage

To run with docker compose build the image using the Dockerfile and add to compose.yaml:
```bash
services:
   earthquake-data-layer:
    image: {YOUR_IMAGE_NAME}
    environment:
      DATA_LAYER_ENDPOINT: {DATA_LAYER_ENDPOINT}
      DATA_LAYER_PORT: {DATA_LAYER_PORT}
      AWS_S3_ENDPOINT: {AWS_S3_ENDPOINT}
      AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: {AWS_SECRET_ACCESS_KEY}
      AWS_REGION: {AWS_REGION}
      AWS_BUCKET_NAME: {AWS_BUCKET_NAME}
    expose:
      - {DATA_LAYER_PORT}
```

### Contributing

Contributions are welcome! Please follow the contribution guidelines to contribute to the project.

### License

This project is licensed under the MIT License. Feel free to use and modify the code according to your needs.
