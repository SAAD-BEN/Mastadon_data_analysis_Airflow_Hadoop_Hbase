from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.providers.email.operators.send_email import SendEmail
import subprocess
import sys
sys.path.insert(0, '/home/project/Mastadon_data_analysis_Airflow_Hadoop_Hbase/dataCollection')
sys.path.insert(0, '/home/project/Mastadon_data_analysis_Airflow_Hadoop_Hbase/loadingIntoHbase')
from loadData import retrieve_and_save_mastodon_data
from insertion import insert_data_into_hbase

def success_callback(context):
    SendEmail(
        to=["saad.bentaleb08@gmail.com"],
        subject="Mastodon Data Pipeline Succeeded",
        html_content="The Mastodon Data Pipeline has succeeded."
    )

def failure_callback(context):
    SendEmail(
        to=["saad.bentaleb08@gmail.com"],
        subject="Mastodon Data Pipeline Failed",
        html_content="The Mastodon Data Pipeline has failed."
    )

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 10, 23),
    'retries': 0,  # Set to 0 to disable retries for the DAG
}

with DAG('mastodon_data_pipeline1', default_args=default_args, schedule_interval='@daily') as dag:
    def set_data_path(**kwargs):
        data_path = retrieve_and_save_mastodon_data()  # Run the data collection function
        processed_path = '/processed/' + datetime.now().strftime('%Y-%m-%d/%H-%M') + '/'
        Variable.set("data_path", data_path)
        Variable.set("processed_path", processed_path)

    # Create PythonOperator tasks
    retrieve_and_save_mastodon_data_task = PythonOperator(
        task_id='mastodon_data_pipeline1',
        provide_context=True,
        python_callable=set_data_path,
        dag=dag,  
    )

    def run_map_reduce(**kwargs):
        data_path = Variable.get("data_path")  # Retrieve the data path from the variable
        output_path = Variable.get("processed_path")  # Retrieve the processed path from the variable
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
        dag=dag,  
    )

    def run_hbase_insertion(**kwargs):
        processed_path = Variable.get("processed_path")
        insert_data_into_hbase(processed_path)

    run_hbase_insertion_task = PythonOperator(
        task_id='run_hbase_insertion',
        provide_context=True,
        python_callable=run_hbase_insertion,
        dag=dag,  
    )

    retrieve_and_save_mastodon_data_task >> run_map_reduce_task >> run_hbase_insertion_task

if __name__ == "__main__":
    dag.cli()

