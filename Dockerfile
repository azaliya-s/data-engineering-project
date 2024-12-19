FROM quay.io/astronomer/astro-runtime:12.5.0
RUN pip install dbt-core
RUN pip install dbt-bigquery
USER root
RUN apt-get update && apt-get install -y git
USER astro
# COPY dbt/bitcoin_project/profiles.yml /usr/local/airflow/dbt/profiles.yml
# Удаляем директорию service_account.json, если она есть
# RUN rm -rf /usr/local/airflow/dbt/credentials/service_account.json
# Create the target directory
# RUN mkdir -p /usr/local/airflow/dbt/credentials/
# Копируем файл ключа
# COPY gcp_key.json /usr/local/airflow/dbt/credentials/service_account.json
ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/local/airflow/dbt/credentials/service_account.json"
