# Mastodon Data Pipeline README

This README provides an overview of a data pipeline project that consists of four main phases: data extraction from the Mastodon API, data processing with Hadoop MapReduce using Python streaming, data storage in HBase, and orchestration with Apache Airflow for automated daily execution. The project aims to collect and analyze data from the Mastodon platform, a federated social media platform.

## Folders structure
```
Repository Root
├── dataCollection
│   ├── Commands.txt
│   ├── last_toot_id.txt
│   ├── loadData.py
│   ├── public_posts.json
│   ├── requirements.txt
│   ├── tests.ipynb
│
├── mapReduce
│   ├── Python
│   │   ├── mapper.py
│   │   ├── reducer.py
│
├── airFlowDag
│
├── README.md
```

## Project Overview

As a Data Developer, your role in this project is to set up an automated pipeline to address the following challenges:

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

- **HBase Schema Design:** Design the HBase schema based on the information you want to extract.

- **Best Practices:** Follow best practices for row key design, column family design, compression, bloom filters, batch inserts, etc.

- **Table Creation:** Create the necessary tables in HBase.

- **Data Insertion:** Populate the output from the reducer into HBase tables using a Python HBase client or your preferred method.

### Phase 4: Orchestration with Apache Airflow

- **Workflow Orchestration:** Define a Directed Acyclic Graph (DAG) to orchestrate the entire workflow.

- **Task Creation:** Create tasks for running the MapReduce job and storing results in HBase.

- **Monitoring and Error Handling:** Monitor progress and manage errors or failures.

### Phase 5: Data Analysis

After successfully completing the previous phases, you can perform data analysis. You'll write queries to:

#### User Analysis

- Identify users with the highest number of followers.
- Analyze user engagement rates.
- Analyze user growth over time using the `user_created_at` metric.

#### Content Analysis

- Identify the most shared external websites (URLs).

#### Language and Region Analysis

- Analyze the distribution of posts based on their language.

#### Media Engagement Analysis

- Determine the number of posts with multimedia attachments.

#### Tag and Mention Analysis

- Identify the most frequently used tags and mentioned users.

### Phase 6: Workflow Execution

In the Apache Airflow web interface, activate the DAG, monitor DAG execution progress, and check logs for any issues. Once the DAG is complete, review the results in HBase.

### Phase 7: Optimization and Monitoring

Optimize MapReduce scripts for better performance. Monitor HBase for storage issues and set up alerts in Airflow for task failures. Regularly monitor Hadoop using its respective web interface.

### Phase 8: Data Access Rights Configuration Updates

Update API tokens if organizational roles change, ensuring they have the necessary permissions for data retrieval.

### Phase 9: Documentation Updates

Update access and sharing rules documentation, including details on roles, permissions, access requests, and access granting/revocation processes.

### Phase 10: Scheduling and Compliance

Ensure that DAGs are scheduled at appropriate intervals for data refresh. Update the data processing log to ensure GDPR compliance by documenting all personal data from Mastodon and how it's processed.

Please refer to the old README for an overview of the Mastodon data structure, which provides insight into the format of Mastodon posts and user accounts.

**Note:** The Mastodon data structure mentioned in the old README is a generalized representation, and actual data may vary depending on the instance and post content.