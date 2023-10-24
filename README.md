# Mastodon Data Pipeline README

This README provides an overview of a data pipeline project that consists of four main phases: data extraction from the Mastodon API, data processing with Hadoop MapReduce using Python streaming, data storage in HBase, and orchestration with Apache Airflow for automated daily execution. The project aims to collect and analyze data from the Mastodon platform, a federated social media platform.

## Folders structure
```
Repository Root
├── airFlowDag
│   ├── mastadon_dag.py
│
├── dataCollection
│   ├── last_toot_id.txt
│   ├── loadData.py
│   ├── public_posts.json
│   ├── tests.ipynb
│
├── images
│   ├── airflow.PNG
│   ├── gantt.PNG
│   ├── workflow.PNG
│
├── loadingIntoHbase
│   ├── insertion.py
│
├── mapReduce
│   ├── Python
│   │   ├── mapper.py
│   │   ├── reducer.py
│
├── airFlowDag
│   ├── mastadon_dag.py
│
│
├── README.md
├── analysis.ipynb
├── Commands.txt
├── RGPD.pdf
├── requirements.txt

```

## Project Overview


As a Data Developer, your role in this project is to set up an automated pipeline to address the following challenges:
---
![Workflow](/images/workflow.png)
---

### Phase 1: Data Collection

- **Data Extraction:** Utilize the Mastodon API with your access tokens to gather raw data from the Mastodon platform.

- **Raw Data Storage:** Store the raw data in a distributed file system such as HDFS for scalability.

- **HDFS Data Lake Modeling:** Define the data lake schema for HDFS.

### Phase 2: Data Processing with MapReduce

- **Mapper:** Process the input data and generate key-value pairs based on the desired metrics (e.g., user followers, engagement rate, URLs, emojis, etc.).

- **Reducer:** Aggregate the key-value pairs produced by the mapper.

- **MapReduce Job Execution:** Use Hadoop streaming API to execute the MapReduce task, providing the mapper and reducer scripts as inputs.

- **Monitoring:** Keep track of job progress through the Hadoop web UI.

### Phase 3: Data Storage in HBase

- **HBase Schema Design:** Design the HBase tables schema based on the information you want to extract.

| **Table Name**           | **Description**                               | **Schema**                                 |
|--------------------------|-----------------------------------------------|-------------------------------------------|
| `language_table`         | Stores language counts.                      | - Row Key: Language code (e.g., 'en', 'es')<br>- Columns:<br>  - `data:count`: Count of users/data.<br>- Timestamp: Record timestamp.        |
| `user_table`             | Stores user information, engagement rates, and followers. | - Row Key: User ID (e.g., '1007156', '10106')<br>- Columns:<br>  - `data:engagement_rate`: User's engagement rate.<br>  - `data:followers`: Number of followers.<br>- Timestamp: Record timestamp. |
| `croissance_table`       | Records user creation counts by month.       | - Row Key: Month (e.g., '2007-07', '2008-01')<br>- Columns:<br>  - `data:count`: Count of user creations.<br>- Timestamp: Record timestamp.   |
| `url_table`              | Tracks mentions of external websites.        | - Row Key: Website URL (e.g., 'a.gup.pe', 'abcgazetesi.com')<br>- Columns:<br>  - `data:count`: Count of mentions.<br>- Timestamp: Record timestamp. |
| `toot_with_media_table`  | Records the count of toots with media content. | - Row Key: Fixed key 'toot_with_media'<br>- Columns:<br>  - `data:count`: Count of toots with media.<br>- Timestamp: Record timestamp.   |
| `tag_table`              | Stores counts of used tags.                  | - Row Key: Tag name (e.g., '10yrsago', '17thcentury')<br>- Columns:<br>  - `data:count`: Count of tag usage.<br>- Timestamp: Record timestamp. |


- **Best Practices:** Follow best practices for row key design, column family design, compression, bloom filters, batch inserts, etc.

- **Table Creation:** Create the necessary tables in HBase.

```
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

connection.close()
```

- **Data Insertion:** Populate the output from the reducer into HBase tables using a Python HBase client or your preferred method.

### Phase 4: Orchestration with Apache Airflow

- **Workflow Orchestration:** Define a Directed Acyclic Graph (DAG) to orchestrate the entire workflow.

- **Task Creation:** Create tasks for running the MapReduce job and storing results in HBase.

- **Monitoring and Error Handling:** Monitor progress and manage errors or failures.

### Phase 5: Data Analysis

After successfully completing the previous phases, you can perform data analysis. You

### Phase 6: Workflow Execution

In the Apache Airflow web interface, activate the DAG, monitor DAG execution progress, and check logs for any issues. Once the DAG is complete, review the results in HBase.

#### Airflow run details

![Airflow Run](/images/airflow.PNG)

### Phase 7: Optimization and Monitoring

Optimize MapReduce scripts for better performance. Monitor HBase for storage issues and set up alerts in Airflow for task failures. Regularly monitor Hadoop using its respective web interface.

### Phase 8: Data Access Rights Configuration Updates

Update API tokens if organizational roles change, ensuring they have the necessary permissions for data retrieval.

### Phase 9: Scheduling and Compliance

Ensure that DAGs are scheduled at appropriate intervals for data refresh. Update the data processing log to ensure GDPR compliance by documenting all personal data from Mastodon and how it's processed.
