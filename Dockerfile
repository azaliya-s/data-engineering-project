FROM quay.io/astronomer/astro-runtime:12.5.0
RUN pip install dbt-core
RUN pip install dbt-bigquery
USER root
RUN apt-get update && apt-get install -y git
USER astro
COPY dbt/bitcoin_project/profiles.yml /usr/local/airflow/dbt/profiles.yml
ARG GCP_KEY_PATH
COPY ${GCP_KEY_PATH} /usr/local/airflow/dbt/credentials/service_account.json
ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/local/airflow/dbt/credentials/service_account.json"
