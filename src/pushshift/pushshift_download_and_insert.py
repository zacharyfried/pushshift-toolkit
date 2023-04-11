import os
import argparse
import subprocess
import json
import pymysql
from datetime import datetime

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

    # Connect to the database
    cnx = pymysql.connect(read_default_file='/home/zfried/.my.cnf', host='127.0.0.1', database='zfried')
    cursor = cnx.cursor()

    # Iterate over the years and months
    year = start_year
    month = start_month
    while (year < end_year) or (year == end_year and month <= end_month):
        if download_type in ('both', 'submissions'):
            # Download the submission file if needed
            url = submission_url.format(year=year, month=month)
            file_name = 'submissions/RS_{year}-{month:02d}.zst'.format(year=year, month=month)
            if os.path.isfile(file_name):
                print(f"Found {file_name} in current directory. Skipping download")
            else:
                print('Downloading submissions: ' + 'RS_{year}-{month:02d}.zst'.format(year=year, month=month))
                subprocess.call(['wget', '-O', file_name, url])

            # Decompress the file
            decompressed_file_name = file_name.replace('.zst', '')
            subprocess.call(['unzstd', file_name, '--memory=2048MB'])

            # Create table in MySQL database for this submission file
            table_name = "sub_{}_{:02d}".format(year, month)
            create_table_query = "CREATE TABLE IF NOT EXISTS {} LIKE zfried.sub_template".format(table_name)
            cursor.execute(create_table_query)

            # Disable keys in table
            disable_keys_query = "ALTER TABLE {} DISABLE KEYS".format(table_name)
            cursor.execute(disable_keys_query)

            # Read the decompressed file and insert the data into the MySQL database
            with open(decompressed_file_name, 'r') as f:
                chunk =[]
                failed_inserts = []
                for line in f:
                    data = json.loads(line)
                    message_id = data['id']
                    user_id = data['author']
                    message = data['selftext']
                    created_utc = datetime.utcfromtimestamp(data['created_utc']).strftime('%Y-%m-%d %H:%M:%S')
                    subreddit = data.get('subreddit', None)
                    subreddit_id = data.get('subreddit_id', None)
                    author_created_utc = data.get('author_created_utc', None)
                    score = data.get('score', None)
                    permalink = data.get('permalink', None)
                    author_flair_text = data.get('author_flair_text', None)
                    total_awards_received = data.get('total_awards_received', None)
                    num_comments = data.get('num_comments', None)
                    title = data.get('title', None)

                    # Add line to chunk
                    row = (message_id, user_id, message, created_utc, subreddit, subreddit_id, author_created_utc, score, permalink, author_flair_text, total_awards_received, num_comments, title)
                    chunk.append(row)

                    # Insert chunk into table when max chunk size is reached
                    if len(chunk) == 1000:
                        insert_data_query = "INSERT INTO {} (message_id, user_id, message, created_utc, subreddit, subreddit_id, author_created_utc, score, permalink, author_flair_text, total_awards_received, num_comments, title) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_name)
                        
                        # Attempt insertion of chunk into table
                        try:
                            cursor.executemany(insert_data_query, chunk)
                            cnx.commit()
    
                        # Insert rows one at a time if a row within the chunk causes a DataError
                        except pymysql.DataError:
                            cnx.rollback()
                            for entry in chunk:
                                message_id = entry[0]
                                user_id = entry[1]
                                message = entry[2]
                                created_utc = entry[3]
                                subreddit = entry[4]
                                subreddit_id = entry[5]
                                author_created_utc = entry[6]
                                score = entry[7]
                                permalink = entry[8]
                                author_flair_text = entry[9]
                                total_awards_received = entry[10]
                                num_comments = entry[11]
                                title = entry[12]
                                try:
                                    cursor.execute(insert_data_query, (message_id, user_id, message, created_utc, subreddit, subreddit_id, author_created_utc, score, permalink, author_flair_text, total_awards_received, num_comments, title))
                                except pymysql.DataError:
                                    failed_inserts.append(entry)
                                    cnx.rollback()
                            cnx.commit()

                        # Reset chunk
                        chunk = []
                
                # Insert remaining rows into table
                if chunk:
                    cursor.executemany(insert_data_query, chunk)
                    cnx.commit()

                # Write entries that were not inserted into MySQL to a txt file
                with open(f"{decompressed_file_name}_failed_inserts.txt", 'w') as f: 
                    for item in failed_inserts:
                        f.write(str(item) + '\n')

                # Enable keys in table
                enable_keys_query = "ALTER TABLE {} ENABLE KEYS".format(table_name)
                cursor.execute(enable_keys_query)

                with open("{}.done".format(decompressed_file_name), "w") as f:
                    f.write("Processed")

                # Delete the downloaded .zst file
                os.remove(file_name)

                # Delete the decompressed file
                os.remove(decompressed_file_name)

                print("Finished inserting data into table {}".format(table_name))

        if download_type in ('both', 'comments'):
            # Download the comment file
            url = comment_url.format(year=year, month=month)
            file_name = 'comments/RC_{year}-{month:02d}.zst'.format(year=year, month=month)
            if os.path.isfile(file_name):
                print(f"Found {file_name} in current directory. Skipping download")
            else:
                print('Downloading comments: ' + 'RC_{year}-{month:02d}.zst'.format(year=year, month=month))
                subprocess.call(['wget', '-O', file_name, url])

            # Decompress the file
            decompressed_file_name = file_name.replace('.zst', '')
            subprocess.call(['unzstd', file_name, '--memory=2048MB'])

            # Create table in MySQL database for this comment file
            table_name = "com_{}_{:02d}".format(year, month)
            create_table_query = "CREATE TABLE IF NOT EXISTS {} LIKE zfried.com_template".format(table_name)
            cursor.execute(create_table_query)

            # Disable keys in table
            disable_keys_query = "ALTER TABLE {} DISABLE KEYS".format(table_name)
            cursor.execute(disable_keys_query)

            # Read the decompressed file and insert the data into the MySQL database
            with open(decompressed_file_name, 'r') as f:
                chunk = []
                failed_inserts = []
                for line in f:
                    data = json.loads(line)
                    message_id = data['id']
                    user_id = data['author']
                    message = data['body']
                    created_utc = datetime.utcfromtimestamp(data['created_utc']).strftime('%Y-%m-%d %H:%M:%S')
                    subreddit = data.get('subreddit', None)
                    subreddit_id = data.get('subreddit_id', None)
                    author_created_utc = data.get('author_created_utc', None)
                    controversiality = data.get('controversiality', None)
                    link_id = data.get('link_id', None)
                    parent_id = data.get('parent_id', None)
                    is_submitter = data.get('is_submitter', None)
                    score = data.get('score', None)
                    permalink = data.get('permalink', None)

                    # Add row to chunk
                    row = (message_id, user_id, message, created_utc, subreddit, subreddit_id, author_created_utc, controversiality, link_id, parent_id, is_submitter, score, permalink)
                    chunk.append(row)

                    # Insert chunk into table when max chunk size is reached
                    if len(chunk) == 1000:
                        insert_data_query = "INSERT INTO {} (message_id, user_id, message, created_utc, subreddit, subreddit_id, author_created_utc, controversiality, link_id, parent_id, is_submitter, score, permalink) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_name)

                        # Attempt insertion of chunk into table
                        try:
                            cursor.executemany(insert_data_query, chunk)
                            cnx.commit()

                        # Insert rows one at a time if a row within the chunk causes a DataError
                        except pymysql.DataError:
                            cnx.rollback()
                            for entry in chunk:
                                message_id = entry[0]
                                user_id = entry[1]
                                message = entry[2]
                                created_utc = entry[3]
                                subreddit = entry[4]
                                subreddit_id = entry[5]
                                author_created_utc = entry[6]
                                controversiality = entry[7]
                                link_id = entry[8]
                                parent_id = entry[9]
                                is_submitter = entry[10]
                                score = entry[11]
                                permalink = entry[12]
                                try:
                                    cursor.execute(insert_data_query, (message_id, user_id, message, created_utc, subreddit, subreddit_id, author_created_utc, controversiality, link_id, parent_id, is_submitter, score, permalink))
                                except pymysql.DataError:
                                    failed_inserts.append(entry)
                                    cnx.rollback()
                            cnx.commit()

                        # Reset chunk
                        chunk = []

                # Insert remaining rows into table
                if chunk:
                    cursor.executemany(insert_data_query, chunk)
                    cnx.commit()

                # Write entries that were not inserted into MySQL to a txt file
                with open(f"{decompressed_file_name}_failed_inserts.txt", 'w') as f: 
                    for item in failed_inserts:
                        f.write(str(item) + '\n')

                # Enable keys in table
                enable_keys_query = "ALTER TABLE {} ENABLE KEYS".format(table_name)
                cursor.execute(enable_keys_query)

                with open("{}.done".format(decompressed_file_name), "w") as f:
                    f.write("Processed")

                # Delete the downloaded .zst file
                os.remove(file_name)

                # Delete the decompressed file
                os.remove(decompressed_file_name)
                
                print("Finished inserting data into table {}".format(table_name))

        # Increment the month and year
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    cursor.close()
    cnx.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download and process Reddit data from Pushshift')
    parser.add_argument('start_year', type=int, help='The start year of the data to download')
    parser.add_argument('start_month', type=int, help='The start month of the data to download')
    parser.add_argument('end_year', type=int, help='The end year of the data to download')
    parser.add_argument('end_month', type=int, help='The end month of the data to download')
    parser.add_argument('download_type', choices=['both', 'submissions', 'comments'], help='The type of data to download (submissions, comments, or both)')
    args = parser.parse_args()
    download_data(args.start_year, args.start_month, args.end_year, args.end_month, args.download_type)

