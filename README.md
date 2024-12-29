Overview
========
This repository demonstrates an end-to-end data pipeline using Airflow (deployed on Astronomer), GitHub Actions for CI/CD, and Google Cloud services (GCS & BigQuery). The pipeline fetches bitcoin data from a public API, stores it in Google Cloud Storage, then loads it into BigQuery for analytics. Finally, data is visualized and analyzed using Looker Studio.



Project Contents
================

1. Architecture Overview
2. Key Components
3. Setup & Installation
4. Running the Pipeline
5. CI/CD with GitHub Actions
6. Looker Studio Dashboard
7. Contributing

Architecture Overview
===========================

1. Data Source (API): The pipeline fetches data from CoinGecko API.
2. Airflow DAG: Hosted on Astronomer, orchestrating data retrieval and loading.
3. Google Cloud Storage (GCS): Temporary or long-term storage for raw or intermediate data.
4. BigQuery: Data warehouse where the pipeline loads structured data for queries.
5. Looker Studio: A dashboard tool for analyzing and visualizing the ingested data.
   
(CoinGecko API) --> Airflow (Astronomer) --> GCS --> BigQuery --> Looker Studio

Key Components
=================================

- Astronomer Airflow: Schedules and manages the data ingestion, transformation, and loading.
- GitHub Actions: Provides CI/CD to test and deploy Airflow DAGs automatically.
- Google Cloud Storage (GCS): Acts as a staging area for raw or intermediate data files.
- BigQuery: Houses the final processed data for analysis.
- Looker Studio: Visualizes data via dashboards and reports.


Setup & Installation
=================================

1. Clone the Repository
   
```git clone https://github.com/azaliya-s/data-engineering-project.git```

```cd [repository-name]```

2. Install Python Dependencies (Optional for Local Testing)
```pip install -r requirements.txt```

4. Astronomer Account & CLI
Sign up or log in to Astronomer.
(Optional) Install the Astronomer CLI locally with ```curl -sSL https://install.astronomer.io | sudo bash -s```.
Make sure you have a deployment set up on Astronomer.

6. Google Cloud Setup
Create a GCS bucket and a BigQuery dataset.
Generate a service account key with the necessary permissions (Cloud Storage & BigQuery).
Add this key to your GitHub Repo Settings → Secrets and variables → Actions (e.g., GCP_CREDENTIALS).


Running the Pipeline
=================================

1. Local (Optional)
- You can run Airflow locally using the Astronomer CLI (```astro dev start```) if you want to test locally.
- Access the Airflow webserver at ```http://localhost:8080``` and trigger the DAG manually.
2. Deploy on Astronomer
- Commit and push your code to the ```main``` branch (or whatever branch triggers your CI/CD).
- GitHub Actions will test and deploy your Airflow project to Astronomer automatically (assuming your secrets are configured).
3. Data Flow
- The Airflow DAG fetches bitcoin price data from the CoinGecko API.
- The data is uploaded to Google Cloud Storage.
- A subsequent task loads the data from GCS into BigQuery.
- You can then query and visualize it in Looker Studio.

CI/CD with GitHub Actions
=================================

- The ```.github/workflows/deploy-to-astro.yaml``` file handles automatic builds and deployments:
  1. Checks out the code.
  2. Parses or tests the DAGs to ensure no syntax errors.
  3. Deploys to Astronomer if everything passes.
- Make sure you have these secrets set in GitHub:
  - ```ASTRO_API_TOKEN``` (your Astronomer API token for deployment).
  - ```GCP_CREDENTIALS``` (JSON for your GCP service account).
  - Any other environment variables your DAG needs (e.g., API keys).

Looker Studio Dashboard
=================================

- After the data is in BigQuery, you can set up a Looker Studio report:
  - Connect BigQuery as the data source.
  - Build custom charts, tables, or metrics to analyze trends and insights.
  - Share or keep the report private as needed.

Contributing
=================================

1. Fork this repo.
2. Create a feature branch for your changes.
3. Submit a pull request with a clear description of your improvements or fixes.



