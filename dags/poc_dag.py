import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

from airflow.providers.postgres.operators.postgres import PostgresOperator
from pytrends.request import TrendReq
import psycopg2

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas

def print_hello():
    return "Hello, Airflow!"

def extract_data(**kwargs):
    api_url = "https://pokeapi.co/api/v2/pokemon?limit=100&offset=0"

    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        results = data['results']

        df = pd.DataFrame(results)

        df['number'] = df['url'].str.extract(r'v2\/pokemon\/(\d+)\/')
        df['name'] = df['name']

        csv_file_path = "/tmp/pokemon_list.csv"

        df.to_csv(csv_file_path, index=False)

        kwargs['ti'].xcom_push(key="pokemon_data_csv", value=csv_file_path)
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")

def insert_into_db(**kwargs):
    print("Hi")
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(key="pokemon_data_csv")

    db_params = {
        "dbname": "pokemon",
        "user": "airflow",
        "password": "airflow",
        "host": "postgres",
        "port": 5432
    }

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    try:
        df = pd.read_csv(csv_file_path)

        insert_query = """
        INSERT INTO pokemon_data (number, name)
        VALUES (%s, %s)
        """

        for index, row in df.iterrows():
            number = row['number']
            name = row['name']
            cursor.execute(insert_query, (number,name))

        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


default_args = {
    'owner': 'yoda45',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Create a DAG instance
dag = DAG('poc_dag',
          default_args=default_args,
          schedule_interval=None)


# Create tasks

extract_data_task = PythonOperator(task_id='extract_task', provide_context=True, python_callable=extract_data, dag=dag)
insert_into_db_task = PythonOperator(task_id='insert_task', python_callable=insert_into_db, dag=dag)


start_task = EmptyOperator(task_id='start', dag=dag)
hello_task = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
end_task = EmptyOperator(task_id='end', dag=dag)

# Define task dependencies
start_task >> hello_task >> extract_data_task >> insert_into_db_task >> end_task
