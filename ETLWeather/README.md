Overview
Welcome to the Astronomer project! This setup was created using the astro dev init command via the Astronomer CLI. The purpose of this project is to help you run Apache Airflow locally and get started with building and managing data pipelines.

What's in This Project
Here’s a quick look at what’s included:

dags/ – This folder contains all your Airflow DAGs. It includes a sample DAG called example_astronauts, which pulls live data from the Open Notify API to list current astronauts in space. It uses Airflow’s TaskFlow API and dynamic task mapping to process each astronaut individually. You can read more about this example in Astronomer’s Getting Started tutorial.

Dockerfile – This defines the Astro Runtime Docker image. You can customize it if you need to run specific commands during runtime.

include/ – A place to put any extra files you might need. It’s currently empty.

packages.txt – Use this to list any system-level packages you want to install. Empty by default.

requirements.txt – Add any Python libraries you need here.

plugins/ – Add any custom or third-party Airflow plugins here.

airflow_settings.yaml – Configure local Airflow settings like connections, variables, and pools in this file instead of doing it manually in the UI.

Running Airflow Locally
To start Airflow on your local machine, just run:

sql
Copy
Edit
astro dev start
This will launch five Docker containers:

Postgres – Stores Airflow metadata.

Scheduler – Monitors DAGs and triggers tasks.

DAG Processor – Parses and processes your DAGs.

API Server – Hosts the Airflow web interface and API.

Triggerer – Handles deferred tasks.
