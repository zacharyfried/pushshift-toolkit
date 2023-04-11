import os
import argparse
import subprocess

def download_data(start_year, start_month, end_year, end_month, download_type):
    # Create directories for submissions and comments
    if not os.path.exists('submissions'):
        os.makedirs('submissions')
    if not os.path.exists('comments'):
        os.makedirs('comments')

    # Define the base URL
    base_url = 'https://files.pushshift.io/reddit/'

    # Define the URLs for submissions and comments
    submission_url = base_url + 'submissions/RS_{year}-{month:02d}.zst'
    comment_url = base_url + 'comments/RC_{year}-{month:02d}.zst'

    # Iterate over the years and months
    year = start_year
    month = start_month
    while (year < end_year) or (year == end_year and month <= end_month):
        if download_type in ('both', 'submissions'):
            # Download the submission file
            url = submission_url.format(year=year, month=month)
            file_name = 'submissions/RS_{year}-{month:02d}.zst'.format(year=year, month=month)
            print('Downloading submissions: ' + 'RS_{year}-{month:02d}.zst'.format(year=year, month=month))
            subprocess.call(['wget', '-O', file_name, url])

        if download_type in ('both', 'comments'):
            # Download the comment file
            url = comment_url.format(year=year, month=month)
            file_name = 'comments/RC_{year}-{month:02d}.zst'.format(year=year, month=month)
            print('Downloading comments :' + 'RC_{year}-{month:02d}.zst'.format(year=year, month=month))
            subprocess.call(['wget', '-O', file_name, url])

        # Increment the month and year
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

if __name__ == '__main__':
    # Parse the command line arguments
    parser = argparse.ArgumentParser(description='Download Reddit data from Pushshift')
    parser.add_argument('start_year', type=int, help='The start year to download')
    parser.add_argument('start_month', type=int, help='The start month to download')
    parser.add_argument('end_year', type=int, help='The end year to download')
    parser.add_argument('end_month', type=int, help='The end month to download')
    parser.add_argument('download_type', choices=['submissions', 'comments', 'both'], help='The type of data to download')
    args = parser.parse_args()

    # Download the data
    download_data(args.start_year, args.start_month, args.end_year, args.end_month, args.download_type)
