from mastodon import Mastodon
from dotenv import load_dotenv
import os
from hdfs import InsecureClient
import datetime

load_dotenv()

# Connect to the mastodon API
mastodon = Mastodon(
    client_id=os.getenv('Client_key'),
    client_secret=os.getenv('Client_secret'),
    access_token=os.getenv('Access_token'),
    api_base_url="https://mastodon.social"
)

# Initialize an HDFS client
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

# get current date and time
now = datetime.datetime.now()
directory_path = '/raw/'+ str(now.year) + '-' + str(now.month) + '-' + str(now.day)

# Check if the directory already exists
if not hdfs_client.status(directory_path, strict=False):
    hdfs_client.makedirs(directory_path)

# Define the HDFS path where you want to save the data
hdfs_path = directory_path + '/' + str(now.hour) + '-' + str(now.minute) + '/mastodon.txt'

public_posts = mastodon.timeline_public(limit=10)

i = 0
# Create a text file to store the Mastodon data
with hdfs_client.write(hdfs_path) as writer:
    for post in public_posts:
        writer.write(f'Post{i}: {post}\n')
        i += 1

print('Data saved successfully to HDFS : ' + hdfs_path)