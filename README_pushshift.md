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