import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def CSVToJson():
    df = pd.read_csv('/Users/adedamolayusuf/airflow/data.csv')
    for i, r in df.iterrows():
        print(r['state'])
    df.to_json('fromAirflow.json', orient='records')

default_args = {
    'owner': 'segun',
    'start_date': dt.datetime(2023, 6, 30),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('newDAG',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5)) as dag:

    print_starting = BashOperator(
        task_id='starting',
        bash_command='echo "I am reading the CSV now....."'
    )

    CSVJson = PythonOperator(
        task_id='convertCSVtoJson',
        python_callable=CSVToJson
    )

    print_starting >> CSVJson


