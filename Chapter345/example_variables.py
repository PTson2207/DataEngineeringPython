from __future__ import print_function

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

#dinh nghia dau vao
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 4, 16),
    'end_date': datetime(2020, 4, 16)    
}

# dinh ngia dag (DAG-Directed Acylic Graph)
dag = DAG('example_variables', 
    schedule_interval="@once", 
    default_args=default_args
    )



dag_config = Variable.get("example_variables_config", deserialize_json=True)
var1 = dag_config["var1"]
var2 = dag_config["var2"]
var3 = dag_config["var3"]

start = DummyOperator(
    task_id="start",
    dag=dag
)

# ten task va chia nhiem vu cho taks 
# ung voi moi taks se co moi nhiem vu khac nhau
t1 = BashOperator(
    task_id="get_dag_config",
    bash_command='echo "{0}"'.format(dag_config),
    dag=dag,
)



t2 = BashOperator(
    task_id="get_variable_value",
    bash_command='echo {{ var.value.var3 }} ',
    dag=dag,
)


t3 = BashOperator(
    task_id="get_variable_json",
    bash_command='echo {{ var.json.example_variables_config.var3 }} ',
    dag=dag,
)

start >> [t1, t2, t3] # cau hinh lai tree