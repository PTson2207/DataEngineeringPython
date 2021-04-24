import pandas  as pd
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta, time

default_args = {
    'owner': 'PhamTrungSon',
    'start_date': datetime(2021, 4, 23),
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}

def cleanScooter():
    df = pd.read_csv('scooter.csv')
    df.drop('region_id', axis=1)
    df.columns = [x.lower() for x in df.columns]
    df['started_at'] = pd.to_dataframe(df['started_at'], format="%m%d%Y %H:%M")
    df.to_csv('cleanScooter.csv')

def fillerData():
    df = pd.read_csv('cleanScooter.csv')
    fromd = '2019-05-03'
    tod = '2019-06-03'
    tofrom = df[(df['started_at']>fromd) & (df['started_at']<tod)]
    tofrom.to_csv('final.csv')

dag = DAG(
    'Pipline_Clean_Data',
    default_args=default_args,
    schedule_interval = timedelta(minutes=5),
    # '0 * * * *',

)
cleandata = PythonOperator(
    task_id='clean',
    python_callable=cleanScooter,
    dag=dag
)
selectdata = PythonOperator(
    task_id='filler',
    python_callable=fillerData,
    dag=dag
)

moviedata = BashOperator(
    task_id='move',
    bash_command='mv /home/h2-server/final.csv /home/h2-server/Desktop',
    dag=dag
)

cleandata >> selectdata >> moviedata
