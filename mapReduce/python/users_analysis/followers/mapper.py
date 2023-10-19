import json
from hdfs import InsecureClient
import datetime

# Get the current date and time
now = datetime.datetime.now()
hdfs_path = '/raw/' + str(now.year) + '-' + str(now.month) + '-' + str(now.day) + '/' 

# Load the data from HDFS
data = []

# Initialize an HDFS client
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

# Mapper
def map_to_followers(data):
    follower_counts = {}
    for toot in data:
        account = toot.get('account')
        if account:
            user_id = account.get('id')
            followers = account.get('followers_count', 0)
            print(user_id, followers)
            # yield user_id, followers

# Get the current date and time
now = datetime.datetime.now()
hdfs_base_path = '/raw/' + str(now.year) + '-' + str(now.month) + '-' + str(now.day) + '/'


# Function to find JSON files in HDFS folders and apply the mapper
def process_hdfs_folders(hdfs_path):
    data = []

    # List folders within the specified HDFS path
    folder_names = hdfs_client.list(hdfs_path)

    for folder_name in folder_names:
        folder_path = hdfs_path + folder_name + '/'

        # List files within the folder
        files = hdfs_client.list(folder_path)

        for file in files:
            if file.endswith('.json'):
                with hdfs_client.read(folder_path + file, encoding='utf-8') as reader:
                    file_content = reader.read()
                    print(file_content)
                    # for line in file_content.split('\n'):
                    #     try:
                    #         data.append(json.loads(line))
                    #     except json.JSONDecodeError as e:
                    #         print(f"Error loading JSON from file {file}: {e}")
                    #         continue
    
    # Apply the mapper function to the data
    # map_to_followers(data)

# Call the function to process HDFS folders and JSON files
process_hdfs_folders(hdfs_base_path)