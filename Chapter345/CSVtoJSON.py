from datetime import date, timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd


def csvTOjson():
    df = pd.read_csv('/home/PTS/Data/data.csv')
    for i, j in df.iterrows():
        print(j['name'])
    df.tojson('fromAirflow.json', orient='records')


default_args = {
    'owner': 'PhamTrungSon',
    'start_date' : datetime(2021, 4, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'MyCSVDAG',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),      

         # '0 * * * *',
)

print_starting = BashOperator(
    task_id='Starting',
    bash_command='echo "I am reading the CSV now..."',
    dag=dag
)

csvJson = PythonOperator(
    task_id='convertCSVtoJSON',
    python_callable=csvTOjson,
    dag=dag,
)

print_starting.set_downstream(csvJson)
csvJson.set_upstream(print_starting)

print_starting >> csvJson