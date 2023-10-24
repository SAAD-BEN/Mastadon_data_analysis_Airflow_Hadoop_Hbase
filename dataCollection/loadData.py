from mastodon import Mastodon
from dotenv import load_dotenv
import os
from hdfs import InsecureClient
import datetime
import time
import json

load_dotenv()

def retrieve_and_save_mastodon_data():
    # Connect to the mastodon API
    mastodon = Mastodon(
        client_id=os.getenv('Client_key'),
        client_secret=os.getenv('Client_secret'),
        access_token=os.getenv('Access_token'),
        api_base_url="https://mastodon.social"
    )

    # Initialize an HDFS client
    hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

    # Get the current date and time
    now = datetime.datetime.now()
    directory_path = '/raw/' + str(now.year) + '-' + str(now.month) + '-' + str(now.day)

    # Check if the directory already exists
    if not hdfs_client.status(directory_path, strict=False):
        hdfs_client.makedirs(directory_path)

    # Define the HDFS path where you want to save the data
    hdfs_path = directory_path + '/' + str(now.hour) + '-' + str(now.minute)

    # Retrieve the last toot ID from a local file or start with None
    try:
        with open('/home/project/Mastadon_data_analysis_Airflow_Hadoop_Hbase/dataCollection/last_toot_id.txt', 'r', encoding='utf-8') as reader:
            last_toot_id = reader.read().strip()
    except FileNotFoundError:
        last_toot_id = None

    public_posts = []

    # Specify the duration of collecting data in minutes
    duration = 20 # Seconds

    # Get the current time
    start_time = time.time()

    while True:
        # Check if 10 minutes have passed
        if time.time() - start_time >= duration:
            break        # Retrieve public posts

        new = mastodon.timeline_public(limit=40, since_id=last_toot_id)
        
        # Append the current run's public posts to the list
        public_posts.extend(new)
        print(f'Number of posts retrieved: {str(len(public_posts))}', end='\r')
        
        # Update the last_toot_id
        if public_posts:
            latest_toot = public_posts[0]
            last_toot_id = str(latest_toot['id'])

    class CustomJSONEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, datetime.datetime):
                # Convert datetime to a string representation
                return o.strftime('%Y-%m-%d %H:%M:%S %z')
            elif hasattr(o, '__dict__'):
                # Handle other objects with __dict__ attribute
                return o.__dict__
            return super().default(o)

    formatted_data = []
    for obj in public_posts:
        formatted_obj = json.dumps(obj, separators=(',', ':'), default=str, cls=CustomJSONEncoder)
        formatted_data.append(formatted_obj)

    # Convert the formatted data to a string
    formatted_data_str = '\n'.join(formatted_data)

    # Save the preprocessed data to HDFS
    with hdfs_client.write(hdfs_path + '-posts.json', encoding='utf-8') as writer:
        writer.write(formatted_data_str)

    print('Data saved successfully to HDFS: ' + hdfs_path + '-posts.json')
    
    # After retrieving the public posts, you can save the latest toot_id to a local file.
    if public_posts:
        latest_toot = public_posts[0]  # Assuming the latest toot is at the first position
        latest_toot_id = latest_toot['id']

        # Convert latest_toot_id to a string
        latest_toot_id_str = str(latest_toot_id)

        # Define the path to the local file
        local_file_path = '/home/project/Mastadon_data_analysis_Airflow_Hadoop_Hbase/dataCollection/last_toot_id.txt'

        # Update or create the local file with the latest_toot_id
        with open(local_file_path, 'w', encoding='utf-8') as writer:
            writer.write(latest_toot_id_str)


    return hdfs_path + '-posts.json'

if __name__ == '__main__':
    retrieve_and_save_mastodon_data()