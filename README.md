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
2. [Main Components](#main-components)
   - [MetadataManager](#metadatamanager)
   - [Downloader](#downloader)
   - [Preprocess](#preprocess)
   - [Validate](#validate)
   - [Storage](#storage)
3. [Getting Started](#getting-started)
   - [Prerequisites](#prerequisites)
   - [Installation](#installation)
4. [Usage](#usage)
5. [Contributing](#contributing)
6. [License](#license)


## Responsibilities

### Dataset Collection

The first aim of this project is to collect a dataset of earthquakes via an API (https://rapidapi.com/dbarkman/api/everyearthquake),
each request made to the API requires a start_date and an end_data parameter to filter returned results. <br>
Due to API request limitations (150 per day) and response data rows (1k), fetching all data at once (4M rows) is not feasible.
In addition, each API response returns the first n results starting with the newest to the oldest, using an offset param to
allow collection of more than {max_allowed_results} results from the same date. <br>
To overcome this data collections will be made in cycles, with root metadata file ([0] in the below data scheme) at its core.
Among other information the metadata file will include 4 important keys:<br>
start_date - defaults to setting.EARLIEST_EARTHQUAKE_DATE<br>
end_date - defaults to {yesterday}<br>
offset - defaults to 1<br>
collection_start_time - defaults to False<br>
When the collection pipeline will initiate the first cycle will begin and the following algorithm will ensure all the data is collected:<br>
* collection_start_time will be set to {today} and data from start_date to {collection_start_time - 1 day} will be gathered by conducting as many runs as needed.<br>
* When all the data will be gathered (end_date = start_date) start_date will be set to {collection_start_time - 1 day} and collection_start_time will be set to False, indicating a new cycle begins.<br>

The process repeats until all the data since {setting.EARLIEST_EARTHQUAKE_DATE} is collected.
The data collection process is designed to span approximately a month to retrieve the entire dataset after which data from that time
will be retrieved until all the data is retrieved.

### Dataset Update

After collecting the initial dataset a model to predict earthquake for the coming week was trained.
The model's predictions will be verified and monitored by calling the API periodically.

### Storage Operations

The system includes functionality for downloading and uploading data to remote storage.<br>
Data will be uploaded to a dedicated bucket in the cloud, in the following the data scheme:
```
{bucket_root}
    [0]metadata.json
    /data
        / raw_data
            / {year}
                [1] {run_id}_data.parquet: contains each run's data (each row is an earthquake)
                .
                .
                .
        / runs
            [2] runs.parquet: contains all the runs' metadata
            / {year}
                [3] {run_id}.parquet: contains each run's responses metadata
                .
                .
                .
```

## Main Components

### MetadataManager
The class responsibilities include fetching, updating, and saving metadata.
It provides methods for retrieving information such as collection dates,
remaining requests for API keys, and updating these values.

### Downloader
This class handles the retrieval of data from an API, managing API keys, generating request parameters,
and executing concurrent requests for efficient data collection.

### Preprocess
The class responsibilities include processing individual responses, aggregating metadata and processed data,
and calculating information for the next run. It facilitates the preparation of data and metadata for
further analysis.

### Validate
Validates run metadata with a series of steps such as scheme validation and missing values identification.

### Storage
Class for handling S3 storage operations.

## Getting Started

### Prerequisites

- poetry (https://python-poetry.org/docs/)
- API key (https://rapidapi.com/dbarkman/api/everyearthquake)
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

3. Configure API access and storage settings in the configuration file .env.
In addition, its worth taking a look setting and definitions to see that the chosen values work for you.

### Usage

to run manually call:

```bash
collect.run_collection(run_id)
````

or via the api: <br>
run earthquake_data_layer.entrypoint
and call using the route
/collect/{run_id}

### Contributing

Contributions are welcome! Please follow the contribution guidelines to contribute to the project.

### License

This project is licensed under the MIT License. Feel free to use and modify the code according to your needs.
