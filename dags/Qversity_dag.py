from datetime import datetime, timedelta, timezone
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from dotenv import load_dotenv
import json
import psycopg2
import uuid
import logging
import requests
import os

def download_from_url():
    url = "https://qversity-raw-public-data.s3.amazonaws.com/mobile_customers_messy_dataset.json"
    response = requests.get(url)
    with open("/opt/airflow/data/raw/mobile_customers_messy_dataset.json", "wb") as f:
        f.write(response.content)
    logging.info("JSON file Downloaded Successfully")

def postgres_connection():
    load_dotenv()

    conn = psycopg2.connect(
        host="postgres",
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    
    cursor = conn.cursor()
    logging.info("Connection Successfull")
    return conn, cursor 

def create_tables(conn, cursor):
    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS public_bronze;
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public_bronze.raw_customers (
            id SERIAL PRIMARY KEY,
            raw_data JSONB,
            ingestion_time TIMESTAMP,
            batch_id UUID,
            source_file TEXT
        );
    """)
    conn.commit()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id BIGINT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone_number TEXT,
            age INT CHECK (age >= 0),
            country VARCHAR(5),
            city TEXT,
            operator TEXT,
            plan_type TEXT,
            monthly_data_gb NUMERIC,
            monthly_bill_usd NUMERIC,
            registration_date DATE,
            status TEXT,
            device_brand TEXT,
            device_model TEXT,
            record_uuid UUID,
            last_payment_date DATE,
            credit_limit NUMERIC,
            data_usage_current_month NUMERIC,
            latitude NUMERIC,
            longitude NUMERIC,
            credit_score INT
        );
    """)
    conn.commit()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contracted_services (
            customer_id BIGINT REFERENCES customers(customer_id),
            service TEXT
        );
    """)
    conn.commit()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS payment_history (
            customer_id BIGINT REFERENCES customers(customer_id),
            payment_date DATE,
            status TEXT,
            amount NUMERIC
        );
    """)
    conn.commit()
    logging.info("Table creation Successfully!")


def bronze_layer():
    logging.info("Bronze layer Started!")
    download_from_url()
    with open('/opt/airflow/data/raw/mobile_customers_messy_dataset.json') as f:
        records = json.load(f)

    conn, cursor = postgres_connection()

    create_tables(conn, cursor)

    ingestion_time = datetime.now(timezone.utc)
    batch_id = str(uuid.uuid4())
    source_file = 'mobile_customers_messy_dataset.json'

    for user in records:
        cursor.execute("""
            INSERT INTO public_bronze.raw_customers (raw_data, ingestion_time, batch_id, source_file)
            VALUES (%s, %s, %s, %s)
        """, (
            json.dumps(user),
            ingestion_time,
            batch_id,
            source_file
        ))

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Bronze layer done!")


default_args = {
    "owner": "qversity",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "qversity_project_JCV",
    default_args=default_args,
    description="Qversity Final Project",
    schedule_interval=timedelta(days=1),
    catchup=False,
)


bronze_layer = PythonOperator(
    task_id="bronze_layer_task", python_callable=bronze_layer, dag=dag
)


bronze_layer
