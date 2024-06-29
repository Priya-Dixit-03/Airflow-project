from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.time_sensor import TimeSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_dag',
    default_args=default_args,
    description='A full example DAG',
    schedule_interval=timedelta(days=1),
)

def print_hello():
    print("Hello World")

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "This is a Bash command!"',
    dag=dag,
)

def push_function(**kwargs):
    kwargs['ti'].xcom_push(key='my_key', value='Hello from XCom!')

def pull_function(**kwargs):
    value = kwargs['ti'].xcom_pull(key='my_key', task_ids='push_task')
    print(f"Pulled value: {value}")

push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_function,
    provide_context=True,
    dag=dag,
)

pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_function,
    provide_context=True,
    dag=dag,
)

time_sensor_task = TimeSensor(
    task_id='wait_for_time',
    target_time='04:18:00',
    dag=dag,
)

hello_task >> bash_task >> push_task >> pull_task
time_sensor_task >> hello_task
