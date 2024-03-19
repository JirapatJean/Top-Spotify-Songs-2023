# Top Spotify Songs 2023 

Welcome to our data engineering project focused on building a robust data pipeline for ETL (Extract, Transform, Load) tasks using PySpark, Google Cloud Platform (GCP), and Airflow. The primary dataset utilized in this project is the `top-spotify-songs-2023` by `nelgiriyewithana`, sourced from Kaggle.

## Overview

The objective of this project is to establish a robust data pipeline capable of handling ETL tasks efficiently. We utilize PySpark for data processing, GCP for cloud infrastructure, and Airflow for workflow orchestration.

## Prerequisites

Before running the pipeline, ensure you have the following prerequisites set up:

1. **Kaggle Account**: Create a Kaggle account if you haven't already. Follow [this guide](https://www.kaggle.com/docs/api) to set up the Kaggle API.

2. **Kaggle API Key**: Generate a Kaggle API key and configure it for authentication.

3. **Python Environment**: Set up a Python environment with necessary dependencies including PySpark, Airflow, and other required libraries.

## Usage

Follow the steps below to run the data pipeline:

1. **Download Dataset from Kaggle**:
   - Run the following command to download the dataset:
     ```
     kaggle datasets download -d nelgiriyewithana/top-spotify-songs-2023
     ```

2. **Setup Airflow DAG**:
   - Define your Airflow DAG (Directed Acyclic Graph) to orchestrate the ETL workflow. Make sure to configure connections to GCP for seamless integration.

3. **Run PySpark Jobs**:
   - Develop PySpark scripts for data processing tasks. Ensure compatibility with your Airflow DAG.

4. **Execute Airflow DAG**:
   - Trigger the Airflow DAG to commence the ETL process. Monitor the progress and handle any potential errors encountered during execution.

## Directory Structure

- `/data`: Directory to store downloaded datasets.
- `/scripts`: PySpark scripts for data processing tasks.
- `/dags`: Airflow DAG definitions.
- `/configs`: Configuration files for pipeline settings.