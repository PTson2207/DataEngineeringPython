from datetime import time, datetime, timedelta
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
import pandas as pd 
import numpy as np 
import psycopg2 as db
from elasticsearch import Elasticsearch 

def queryPostgreSQL():
    conn_string = "dbname='postgres' host='localhost' user='postgres' password='Tuan1211'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select name, city from users", conn)
    df.to_csv('postgresqldata.csv')
    print("---------Data Saved----------")

def insertElasticsearch():
    es = Elasticsearch()
    df = pd.read_csv('postgresqldata.csv')
    for i, j in df.iterrows():
        doc = r.to_json()
        res = es.index(index="frompostgresql",
            doc_type="doc", body=doc)
        print(res)



default_args = {
    'owner': 'PhamTrungSon',
    'start_date': datetime(2021, 4, 22),
    'retries':1,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG(
    'Database_Dag',
    default_args=default_args,
    schedule_interval = timedelta(minutes=5),
    # '0 * * * *',
)

getdata = PythonOperator(
    task_id='QueryPostgreSQL',
    python_callable = queryPostgreSQL,
    dag=dag,
)

insertdata = PythonOperator(
    task_id='InsertDataElasticsearch',
    python_callable = insertElasticsearch,
    dag=dag,
)

getdata >> insertdata
