import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

def queryPostgresql():
    conn_string = "dbname='dataengineering' host='localhost' user='postgres' password='adebanke1'"
    conn = db.connect(conn_string)
    df = pd.read_sql("SELECT name, city FROM users", conn)
    df.to_csv('postgresqldata.csv')
    print("-------Data Saved------")

def insertElasticsearch():
    es = Elasticsearch()
    df = pd.read_csv('postgresqldata.csv')
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="frompostgresql", doc_type="doc", body=doc)
        print(res)

def bashCommand():
    bash_command = "echo 'Running a Bash command'"
    return bash_command

default_args = {
    'owner': 'dammy',
    'start_date': dt.datetime(2023, 6, 30),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('MyDBdag', default_args=default_args, 
schedule_interval=timedelta(minutes=5)) as dag:
    getData = PythonOperator(task_id='QueryPostgreSQL', 
python_callable=queryPostgresql)
    insertData = PythonOperator(task_id='InsertDataElasticsearch', 
python_callable=insertElasticsearch)
    bashOperator = BashOperator(task_id='BashCommand', 
bash_command=bashCommand())

getData >> insertData >> bashOperator

