from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import datetime 
import time
import pendulum

local = pendulum.timezone("Asia/Jakarta")
default_args = {
    'owner': 'munawarimam',
    'start_date': datetime.datetime(2020, 9, 24, tzinfo = local),
    'retries': 1,
    'concurrency': 5
    }
dag_conf = DAG('bigproject', default_args=default_args, schedule_interval='0 8 1-30 * *', catchup=False)

def start():
    print("Mulai!!")

def finish():
    print("Job Anda Sudah Selesai.")

mulai = PythonOperator(task_id='starting_job', python_callable=start, dag=dag_conf)
joindata = BashOperator(task_id='joindata_job', \
    bash_command='spark-submit --master yarn --class com.imam.jointable /home/munawar_imam9/streamtwittertohive', dag=dag_conf)
selesai = PythonOperator(task_id='job_finished', python_callable=finish, dag=dag_conf)

mulai >> joindata >> selesai
