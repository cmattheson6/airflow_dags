from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
# import os

gcs_creds = 'C:/Users/cmatt/Downloads/gce_creds.json'
project_id = 'politics-data-tracker-1'
bucket_name = 'poliviews'
pipeline_name = 'house_members'
blob_name = 'csvs/{0}/{0}_{1}.csv'.format(pipeline_name, date.today())
gcs_path = 'gs://' + bucket_name + '/' + blob_name

project_path = '../poliviews/house_members'
df_path = '/house_members_dataflow/house_members_df.py'
df_setup_path = '/house_members_dataflow/setup.py'

default_args = {
    'owner': 'cmattheson6',
    'depends_on_past': False,
    'email': ['cmattheson6@gmail.com'],
    'email_on_failure': False, # Eventually change to True
    'email_on_retry': False,
    'start_date': datetime(2019, 7, 16), # Change date to be dynamic,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('house_members',
          default_args=default_args,
          schedule_interval='@daily')

# Pipeline skeleton
# Send Pub/Sub message to activate Google Cloud Functions

# THIS IS NOT AT ALL WHAT I NEED HERE; FIGURE OUT HOW TO SEND A PUB/SUB MESSAGE
# t1 = PythonOperator(
#     task_id='scrape_house_members',
#     python_callable=scrape.main,
#     retries=2,
#     dag=dag
# )


t1 = BashOperator(
    task_id='scrape_house_members',
    bash_command='gcloud pubsub topics publish scrapy --message "house members scrape" --project politics-data-tracker-1',
    retries=2,
    dag=dag
)

# Function will run and upload CSV to GCS
# Sleep 30 min

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 30m',
    retries=2,
    dag=dag
)

# Run Dataflow pipeline to process CSV and upload to BigQuery

# t3 = PythonOperator(
#     task_id='house_members_df',
#     python_callable=df.house_members_df.main(), ### Needs to change, need --setup_file= and --experiments=allow_non_updatable_job_parameter
#     retries=2,
#     dag=dag
# )

t3 = BashOperator(
    task_id='df_v2',
    bash_command='py {0} --setup_file={1} \
    --experiments=allow_non_updatable_job_parameter'.format(df_path, df_setup_path),
    dag=dag
)

# Set all options needed to properly run the pipeline. This pipeline will run on Dataflow as a streaming pipeline.

df_options = {
    'streaming': 'False',
    'runner': 'DataflowRunner',
    'project': project_id,
    'temp_location': 'gs://{0}/tmp'.format(project_id),
    'staging_location': 'gs://{0}/staging'.format(project_id)}

# t3 = DataFlowPythonOperator(
#     task_id='df_v3',
#     py_file=project_path+df_path,
#     py_options=['--setup_file={0}'.format(project_path+df_setup_path),
#                 '--experiments=allow_non_updatable_job_parameter'],
#     dataflow_default_options=df_options,
#     dag=dag
# )

# If successful, clean out temp CSV files

t4 = BashOperator(
    task_id='clean_up',
    bash_command='gsutil rm {0}'.format(gcs_path),
    dag=dag
)

# Build task pipeline order

t1 >> t2 >> t3 >> t4