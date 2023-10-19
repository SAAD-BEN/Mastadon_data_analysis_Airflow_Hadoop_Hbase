from mastodon import Mastodon
from dotenv import load_dotenv
import os
from hdfs import InsecureClient
import datetime
import json
from dateutil import tz

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

# Get the current date and time
now = datetime.datetime.now()
directory_path = '/raw/' + str(now.year) + '-' + str(now.month) + '-' + str(now.day)

# Check if the directory already exists
if not hdfs_client.status(directory_path, strict=False):
    hdfs_client.makedirs(directory_path)

# Define the HDFS path where you want to save the data
hdfs_path = directory_path + '/' + str(now.hour) + '-' + str(now.minute)

# Retrieve the last toot ID from a file or start with None
try:
    with open('last_toot_id.txt', 'r') as f:
        last_toot_id = f.read().strip()
except FileNotFoundError:
    last_toot_id = None

# Retrieve public timeline posts since the last fetched toot
public_posts = mastodon.timeline_public(limit=40, since_id=last_toot_id)

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            # Convert datetime to a string representation
            return o.strftime('%Y-%m-%d %H:%M:%S %z')
        elif hasattr(o, '__dict__'):
            # Handle other objects with __dict__ attribute
            return o.__dict__
        return super().default(o)

# Create a JSON file to store the Mastodon data
with hdfs_client.write(hdfs_path + '/mastodon.json', encoding='utf-8') as writer:
    # Serialize public_posts to JSON
    json.dump(public_posts, writer, ensure_ascii=False, indent=4, default=str, cls=CustomJSONEncoder)


print('Data saved successfully to HDFS : ' + hdfs_path + '/mastodon.json')

# After retrieving the public posts, you can save the latest toot_id to the file.
if public_posts:
    latest_toot = public_posts[0]  # Assuming the latest toot is at the first position
    latest_toot_id = latest_toot['id']

    # Convert latest_toot_id to a string
    latest_toot_id_str = str(latest_toot_id)

    #Check if the file exists
    if hdfs_client.status('last_toot_id.txt', strict=False):
        # update the file
        hdfs_client.write('last_toot_id.txt', latest_toot_id_str, encoding='utf-8', overwrite=True)
    else:
        # Write the latest toot_id to the file
        with open('last_toot_id.txt', 'w') as f:
            f.write(latest_toot_id_str)
    