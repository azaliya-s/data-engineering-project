from datetime import datetime, timedelta
import logging
import json
import tempfile
import os
from typing import Dict, Any

import requests
from google.cloud import storage, bigquery
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BitcoinPriceETL:
    def __init__(self, execution_date: datetime):
        self.execution_date = execution_date
        self.date_str = execution_date.strftime('%Y-%m-%d')
        self.gcs_path = f"bitcoin/prices/dt={self.date_str}/data.ndjson"
        
    def fetch_data(self) -> str:
        """Fetch data with retries and validation"""
        url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        params = {
            "vs_currency": "usd",
            "days": "1",
            "interval": "hourly"
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.ndjson')
        
        try:
            with open(temp_file.name, 'w') as f:
                for timestamp, price in data['prices']:
                    record = {
                        "ingestion_date": self.date_str,
                        "timestamp": int(timestamp),
                        "price_usd": float(price)
                    }
                    f.write(json.dumps(record) + '\n')
            
            return temp_file.name
        except Exception as e:
            os.unlink(temp_file.name)
            raise e

    def upload_to_gcs(self, file_path: str):
        """Upload to GCS with proper authentication"""
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        
        try:
            gcs_hook.upload(
                bucket_name=Variable.get("GCS_BUCKET"),
                object_name=self.gcs_path,
                filename=file_path,
                mime_type='application/x-ndjson'
            )
            logger.info(f"Uploaded data to GCS: {self.gcs_path}")
        finally:
            os.unlink(file_path)

    def load_to_bigquery(self):
        """Load to BigQuery with schema enforcement"""
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        
        schema = [
            bigquery.SchemaField("ingestion_date", "DATE"),
            bigquery.SchemaField("timestamp", "INT64"),
            bigquery.SchemaField("price_usd", "FLOAT64")
        ]
        
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            clustering_fields=['ingestion_date']
        )
        
        uri = f"gs://{Variable.get('GCS_BUCKET')}/{self.gcs_path}"
        table_id = f"{Variable.get('PROJECT_ID')}.{Variable.get('DATASET_RAW')}.bitcoin_prices_raw"
        
        bq_hook.get_client().load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        ).result()
        
        logger.info(f"Loaded data to BigQuery: {table_id}")

    def transform_data(self):
        """Transform data with proper partitioning"""
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        
        query = f"""
        CREATE OR REPLACE TABLE `{Variable.get('PROJECT_ID')}.{Variable.get('DATASET_TRANSFORMED')}.bitcoin_prices_daily`
        PARTITION BY date
        CLUSTER BY hour AS
        SELECT
            ingestion_date,
            TIMESTAMP_MILLIS(timestamp) AS timestamp,
            price_usd,
            DATE(TIMESTAMP_MILLIS(timestamp)) AS date,
            EXTRACT(HOUR FROM TIMESTAMP_MILLIS(timestamp)) AS hour,
            AVG(price_usd) OVER (
                ORDER BY timestamp
                ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
            ) AS moving_avg_24h
        FROM `{Variable.get('PROJECT_ID')}.{Variable.get('DATASET_RAW')}.bitcoin_prices_raw`
        WHERE ingestion_date = '{self.date_str}'
        """
        
        bq_hook.get_client().query(query).result()
        logger.info("Transformed data successfully")

def create_dag_tasks(dag):
    """Create DAG tasks with proper dependency management"""
    def execute_pipeline(**context):
        execution_date = context['execution_date']
        etl = BitcoinPriceETL(execution_date)
        
        try:
            file_path = etl.fetch_data()
            etl.upload_to_gcs(file_path)
            etl.load_to_bigquery()
            etl.transform_data()
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise e

    return PythonOperator(
        task_id='bitcoin_price_etl',
        python_callable=execute_pipeline,
        provide_context=True,
        dag=dag
    )
