from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import subprocess
import sys
sys.path.insert(0, '/home/project/Mastadon_data_analysis_Airflow_Hadoop_Hbase/dataCollection')
from loadData import retrieve_and_save_mastodon_data

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 23, 0, 30),  
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'mastodon_data_dag',
    default_args=default_args,
    description='Mastodon Data Collection and MapReduce',
    schedule_interval=None,  # You can set a schedule_interval as needed
    catchup=False,
)

def set_data_path(**kwargs):
    data_path = retrieve_and_save_mastodon_data()  # Run the data collection function
    Variable.set("data_path", data_path)  # Store the data path as a variable

retrieve_and_save_mastodon_data_task = PythonOperator(
    task_id='retrieve_and_save_mastodon_data',
    provide_context=True,
    python_callable=set_data_path,
    dag=DAG,
)

def run_map_reduce(**kwargs):
    data_path = Variable.get("data_path")  # Retrieve the data path from the variable
    output_path = '/processed/' + datetime.now().strftime('%Y-%m-%d/%H-%M') + '-posts.json'
    # Use subprocess to run Hadoop MapReduce job with the provided data path
    hadoop_command = f"hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar " \
                     f"-mapper /home/project/Mastadon_data_analysis_Airflow_Hadoop_Hbase/mapReduce/python/mapper.py " \
                     f"-reducer /home/project/Mastadon_data_analysis_Airflow_Hadoop_Hbase/mapReduce/python/reducer.py " \
                     f"-input {data_path} " \
                     f"-output {output_path}"
    subprocess.run(hadoop_command, shell=True)

run_map_reduce_task = PythonOperator(
    task_id='run_map_reduce',
    provide_context=True,
    python_callable=run_map_reduce,
    dag=DAG,
)

retrieve_and_save_mastodon_data_task >> run_map_reduce_task