FROM quay.io/astronomer/astro-runtime:12.5.0
RUN pip install dbt-bigquery
USER root
RUN apt-get update && apt-get install -y git
USER astro
COPY gcp_key.json /usr/local/airflow/dbt/credentials/service_account.json
