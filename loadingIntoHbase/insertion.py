import sys
from hdfs import InsecureClient
import json
import happybase




# define a function to process the data from the hdf file
def insert_data_into_hbase(data_folder_path):

    # HBase connection settings
    hbase_host = 'localhost'  # Replace with your HBase host
    hbase_port = 9090  # Default HBase port
    # Connect to HBase
    connection = happybase.Connection(host=hbase_host, port=hbase_port)

    tables_list = ["user_table", "croissance_table", "language_table", "toot_with_media_table", "emoji_table", "url_table", "tag_table"]
    tables = connection.tables()
    # remove the b' and ' from the table names
    tables = [table_name.decode() for table_name in tables]

    # Create the tables if they do not exist
    for table_name in tables_list:
        if table_name not in tables:
            connection.create_table(table_name, {'data': dict()})
    # Get the tables
    user_table = connection.table('user_table')
    croissance_table = connection.table('croissance_table')
    language_table = connection.table('language_table')
    toot_with_media_table = connection.table('toot_with_media_table')
    emoji_table = connection.table('emoji_table')
    url_table = connection.table('url_table')
    tag_table = connection.table('tag_table')
    
    # Initialize an HDFS client
    hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')
    # get files from the data folder that start with part
    files = hdfs_client.list(data_folder_path)
    # get oly the files that start with part
    files = [file for file in files if file.startswith('part')]
    # loop through the files
    for file in files:
        # get the file path
        file_path = data_folder_path + '/' + file
        # open the file
        with hdfs_client.read(file_path, encoding='utf-8') as reader:
            # read the file
            data = reader.read()
            # split the file into lines
            lines = data.splitlines()
            # loop through the lines
            for line in lines:
                # split the line into key and value
                key, value = line.strip().split('\t')
                # split the key into key and id                
                # convert the value to a dictionary
                value = eval(value.strip())
                # if the key is user
                if key.startswith('user'):
                    key, id = key.strip().split(':')
                    # get the engagement rate
                    engagement_rate = value['engagement_rate']
                    # get the followers
                    followers = value['followers']
                    user_dict = {
                        "engagement_rate": engagement_rate,
                        "followers": followers
                    }
                    try:
                        # Store the user data in HBase
                        user_table.put(str(id).encode(), {b'data:engagement_rate': str(user_dict["engagement_rate"]).encode()})
                        user_table.put(str(id).encode(), {b'data:followers': str(user_dict["followers"]).encode()})
                    except Exception as e:
                        # Log exceptions to standard error
                        print(f"Error: {str(e)}")
                elif key.startswith('croissance'):
                    key, id = key.strip().split(':')
                    croissance_dict = {
                        "count": value['count']
                    }
                    try:
                        # Store the user data in HBase
                        croissance_table.put(str(id).encode(), {b'data:count': str(croissance_dict["count"]).encode()})
                    except Exception as e:
                        # Log exceptions to standard error
                        print(f"Error: {str(e)}")
                elif key.startswith('language'):
                    key, id = key.strip().split(':')
                    language_dict = {
                        "count": value['count']
                    }
                    try:
                        # Store the user data in HBase
                        language_table.put(str(id).encode(), {b'data:count': str(language_dict["count"]).encode()})
                    except Exception as e:
                        # Log exceptions to standard error
                        print(f"Error: {str(e)}")
                elif key.startswith('toot_with_media'):
                    id = key.strip()
                    toot_with_media_dict = {
                        "count": value['count']
                    }
                    try:
                        # Store the user data in HBase
                        toot_with_media_table.put(str(id).encode(), {b'data:count': str(toot_with_media_dict["count"]).encode()})
                    except Exception as e:
                        # Log exceptions to standard error
                        print(f"Error: {str(e)}")
                elif key.startswith('emoji'):
                    key, id = key.strip().split(':')
                    emoji_dict = {
                        "count": value['count']
                    }
                    try:
                        # Store the user data in HBase
                        emoji_table.put(str(id).encode(), {b'data:count': str(emoji_dict["count"]).encode()})
                    except Exception as e:
                        # Log exceptions to standard error
                        print(f"Error: {str(e)}")
                elif key.startswith('website'):
                    key, id = key.strip().split(':')
                    url_dict = {
                        "count": value['count']
                    }
                    try:
                        # Store the user data in HBase
                        url_table.put(str(id).encode(), {b'data:count': str(url_dict["count"]).encode()})
                    except Exception as e:
                        # Log exceptions to standard error
                        print(f"Error: {str(e)}")
                elif key.startswith('tag'):
                    key, id = key.strip().split(':')
                    tag_dict = {
                        "count": value['count']
                    }
                    try:
                        # Store the user data in HBase
                        tag_table.put(str(id).encode(), {b'data:count': str(tag_dict["count"]).encode()})
                    except Exception as e:
                        # Log exceptions to standard error
                        print(f"Error: {str(e)}")
                        
    
    connection.close()
                
        
if __name__ == "__main__":
    # Get the data path from the command line
    data_path = sys.argv[1]
    # Process the data
    insert_data_into_hbase(data_path)