import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import date, timedelta

# lay data tu tweets
def fetchtweets():
    return None

# lam sach data
def cleantweets():
    return None

# phan tich data
def analyzetweets():
    return None

# chuyn data vao database
def transfertodb():
    return None


# dinh nghia dau vao
default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# dinh nghia cho dag
dag = DAG(
    'example_tweetter_dag',
    default_args=default_args,
    schedule_interval="@daily"
)

# taks này sê gọi tweeter API và truy xuât cấc tweeter tư hôm qua
# cho 4 ngùơi dùng A-D 


fetch_tweets = PythonOperator(
    task_id = "fetch_tweets",
    python_callable = fetchtweets,
    dag = dag
)

# lam sach tam tep, trong buoc nay ta co the bo hoac chon cac phan khac nhau
# cua van ban
clean_tweets = PythonOperator(
    task_id = "clean_tweets", 
    python_callable = cleantweets,
    dag=dag
)

clean_tweets.set_upstream(fetch_tweets)

# Trong phan nay co the su dung mot tap lenh de phan tich du lieu tweets
# co the don gian la cac thuat toan nham phan tich tinh cam mot cach don gian cho den phuc tap
analyze_tweets = PythonOperator(
    task_id = "analyze_tweets",
    python_callable = analyzetweets,
    dag=dag
)
analyze_tweets.set_upstream(clean_tweets)

# phan nay se trich suat tom tat tu du lieu vao Hive va luu tru no vao MySQL

hive_to_mysql = PythonOperator(
    task_id = "hive_to_mysql",
    python_callable = transfertodb,
    dag=dag
)

# phan nay se chuyen 8 task vao HDFS su dung vong lap 

from_channels = ['fromTwitter_A', 'fromTwitter_B', 'fromTwitter_C', 'fromTwitter_D']
to_channels = ['toTwitter_A', 'toTwitter_B', 'toTwitter_C', 'toTwitter_D'] 
yesterday = date.today() - timedelta(days=1)
dt = yesterday.strftime("%Y-%m-%d")

# dinh nghia thu muc, du lieu cuc bo se duoc chuyen toi noi nay
local_dir = "/tmp/"
# dinh nghia thu muc ma ban muon chuyen den HDFS
hdfs_dir = "/tmp/"

for chanel in to_channels:
    file_name = "to_" + chanel + "_" + yesterday.strftime("%y-%m-%d") + ".csv"

    load_to_hdfs = BashOperator(
        task_id= "put_" + chanel + "_to_hdfs",
        bash_command= "HADOOP_USER_NAME=hdfs hadoop fs -put -f" +
                        local_dir + file_name + hdfs_dir + chanel + "/",
        dag = dag
    )
    load_to_hdfs.set_upstream(analyze_tweets)

    load_to_hive = HiveOperator(
        task_id="load_" + channel + "_to_hive",
        hql="LOAD DATA INPATH '" +
            hdfs_dir + channel + "/" + file_name + "' "
            "INTO TABLE " + channel + " "
            "PARTITION(dt='" + dt + "')",
        dag=dag)
    load_to_hive.set_upstream(load_to_hdfs)
    load_to_hive.set_upstream(hive_to_mysql)

for chanel in from_channels:
    file_name = "from_" + chanel + "_" + yesterday.strftime("%Y-%m-%d") + ".csv"
    load_to_hdfs = BashOperator(
        task_id = "put_" + chanel + "_to_hdfs",
        bash_command = "HADOOP_USER_NAME=hdfs hadoop fs -put -f" +
                        local_dir + file_name + hdfs_dir + chanel + "/",
        dag=dag
    )
    load_to_hdfs.set_upstream(analyze_tweets)

    load_to_hive = HiveOperator(
        task_id = "load_" + chanel + "_to_hive",
        hql="LOAD DATA INPATH '" +
            hdfs_dir + channel + "/" + file_name + "' "
            "INTO TABLE " + channel + " "
            "PARTITION(dt='" + dt + "')",
        dag=dag
    )

load_to_hive.set_upstream(load_to_hdfs)
load_to_hive.set_downstream(hive_to_mysql)