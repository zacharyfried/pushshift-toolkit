## `pushshift_download_and_insert.py`

## Overview

This code downloads and processes Reddit data from the Pushshift API. The data can either be submissions or comments, or both. The data is then decompressed and processed into a MySQL database.

## Usage

```bash
python download_data.py [start_year] [start_month] [end_year] [end_month] [download_type]
```

where `[start_year]` and `[start_month]` specify the start year and month of the data to download, `[end_year]` and `[end_month]` specify the end year and month of the data to download, and `[download_type]` is either 'submissions', 'comments', or 'both', depending on the type of data to download.

## Data Processing

The code downloads .zst files from the Pushshift API, and decompresses them using the `unzstd` utility. The decompressed files are then processed and inserted into a MySQL database.

The submissions data is processed into a table named `sub_{year}_{month:02d}` where `year` and `month` represent the year and month of the data, and `{month:02d}` is the month with a leading zero if needed. The comments data is processed into a table named `com_{year}_{month:02d}`.

After the data is inserted into the database, a `.done` file is created in the same directory as the decompressed file. The decompressed file and the original .zst file are then deleted.

## Dependencies

This code requires the following dependencies:

- Python 3
- pymysql
- wget
- unzstd

## Configuration

The code uses a MySQL database, and requires a configuration file located at `/home/<user>/.my.cnf` with the following contents:

```bash
[client]
user=<username>
password=<password>
```

where `<username>` and `<password>` are the MySQL username and password. Make sure to change the name of the database you would like to use as your destination for the Pushshift data on line 23.







## `download_pushshift_data.py`

This script is used to download Reddit data from Pushshift for a specified time period. The data is either comments or submissions and is stored in .zst format.

### Usage

```bash

python download_pushshift_data.py --start_year <start_year> --start_month <start_month> --end_year <end_year> --end_month <end_month> --download_type <comments or submissions or both>

```

### Arguments
- `start_year`: The year of the earliest data to be downloaded.
- `start_month`: The month of the earliest data to be downloaded (1-12).
- `end_year`: The year of the latest data to be downloaded.
- `end_month`: The month of the latest data to be downloaded (1-12).
- `download_type`: The type of data to be downloaded. Can be either `comments`, `submissions`, or `both`.

### Output
The script will download the .zst files for the specified time period and type of data to the local file system. The files will be stored in either the `submissions` or `comments` directory, depending on the type of data being downloaded. If the `download_type` is set to `both` , files will be stored in both directories. The files will be named in the format `RC_YYYY-MM.zst`  for comments and `RS_YYYY-MM.zst` for submissions.

### Example
Here's an example of how to use the script to download comments for the period from January 2006 to October 2007:

```bash

python download_pushshift_data.py --start_year 2006 --start_month 1 --end_year 2007 --end_month 10 --download_type comments

```