# GeoCognition
Data analytics platform for earth data


# Setting Up

## Required Installations
 - Docker Desktop
 - Python
 - Astro

Virtual environment

```
conda create -p venv python==3.10 -y
conda activate venv/
pip install -r requirements.txt
```

# Docker Runs (Locally)

## Environment Variables (If Running Locally):

1. If you don't have one, create an `.env` file in the root directory of this project. In the file, add:

```bash
MINIO_ACCESS_KEY="minio"
MINIO_SECRET_KEY="minio123"
AIRFLOW_CONN_USGS_API=http://earthquake.usgs.gov
AIRFLOW_CONN_GEOCOG_POSTGRES_DB=postgresql://postgres:postgres@host.docker.internal:5433/geocog-postgres
```

## Run PostGIS + MinIO with Docker Compose

From the project root, start both data services:

```bash
docker compose up -d
```

This project compose file exposes:

- PostGIS on `localhost:5433` (container DB name: `earthquakes`)
- MinIO API on `localhost:9000`
- MinIO Console on `localhost:9001`

## Starting Program

Run these commands in order from the project root:

1. Start data services (PostGIS + MinIO)
```bash
docker compose up -d
```

2. Start Airflow with Astro. Astro CLI runs Airflow in its own Docker-managed stack (webserver, scheduler, triggerer, and metadata PostGIS).
```bash
astro dev start
```

3. On your browser, search up `http://localhost:8080`

## Stopping Program

1. To stop Astro:
```bash
astro dev stop
```

2. To stop PostGIS + MinIO
```bash
docker compose down
```

## Stopping Program (Deleting all data)

Use this when you want a clean local restart.

1. Stop and reset Astro containers
```bash
astro dev kill
```

2. Stop and remove compose services and volumes
```bash
docker compose down -v
```

3. Start again using the first-time startup steps

# Pushing Code

Before pushing code to GitHub, run the following commands

```bash
# Finds problems (bugs, bad patterns, lint issues), while fixes what it can (ignore .ipynb inside of prototype/)
ruff check . --fix --exclude prototype 

# Fixes formatting (style only, not logic)
black .

# Unit testing
pytest 
```