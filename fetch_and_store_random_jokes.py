from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import time
import pytz

# API URL
API_URL = "https://official-joke-api.appspot.com/random_joke"

# Function to fetch joke
def fetch_joke():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching joke: {e}")
        return None

# Function to fetch and store jokes
def fetch_and_store_jokes(ti):
    jokes = []
    start_time = time.time()
    
    while time.time() - start_time < 10:  # Run for 10 seconds
        joke = fetch_joke()
        if joke:
            # Get current time in IST
            ist = pytz.timezone('Asia/Kolkata')
            current_time_ist = datetime.now(ist)
            
            # Add timestamp to joke data
            joke['timestamp'] = current_time_ist.strftime("%Y-%m-%d %H:%M:%S %Z")
            jokes.append(joke)
        
        time.sleep(0.5)  # Wait for 0.5 seconds before next fetch
    
    ti.xcom_push(key='jokes', value=jokes)

# Function to insert jokes into PostgreSQL
def insert_jokes_to_postgres(ti):
    jokes = ti.xcom_pull(key='jokes', task_ids='fetch_and_store_jokes')
    if not jokes:
        print("No jokes to insert")
        return

    postgres_hook = PostgresHook(postgres_conn_id='airflow_connection')
    insert_query = """
    INSERT INTO random_joke_api (type, setup, punchline, joke_id, timestamp)
    VALUES (%s, %s, %s, %s, %s)
    """
    for joke in jokes:
        postgres_hook.run(insert_query, parameters=(
            joke['type'],
            joke['setup'],
            joke['punchline'],
            joke['id'],
            joke['timestamp']
        ))

# Default DAG arguments
default_args = {
    'owner': 'pranavVerma',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'fetch_and_store_random_jokes',
    default_args=default_args,
    description='A DAG to fetch jokes from API and store in PostgreSQL',
    schedule_interval=timedelta(seconds=10),
    catchup=False,
    tags=['api', 'jokes', 'postgres'],
)

# Task to create the table
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='airflow_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS random_joke_api (
        id SERIAL,
        joke_id INTEGER,
        type TEXT,
        setup TEXT,
        punchline TEXT,
        timestamp TIMESTAMP WITH TIME ZONE
    );
    """,
    dag=dag,
)

# Task to fetch and store jokes
fetch_and_store_jokes_task = PythonOperator(
    task_id='fetch_and_store_jokes',
    python_callable=fetch_and_store_jokes,
    dag=dag,
)

# Task to insert jokes into PostgreSQL
insert_jokes_task = PythonOperator(
    task_id='insert_jokes',
    python_callable=insert_jokes_to_postgres,
    dag=dag,
)

# Set task dependencies
create_table_task >> fetch_and_store_jokes_task >> insert_jokes_task