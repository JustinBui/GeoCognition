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
AIRFLOW_CONN_USGS_POSTGRES_DB=postgresql://postgres:postgres@host.docker.internal:5433/earthquakes
```

## Run Postgres + MinIO with Docker Compose

From the project root, start both data services:

```bash
docker compose up -d
```

This project compose file exposes:

- Postgres on `localhost:5433` (container DB name: `earthquakes`)
- MinIO API on `localhost:9000`
- MinIO Console on `localhost:9001`

## First-Time Startup (From Scratch)

Run these commands in order from the project root:

1. Start data services (Postgres + MinIO)
```bash
docker compose up -d
```

2. Start Airflow with Astro
```bash
astro dev start
```

3. Create the target table in Postgres (one time)
```powershell
Get-Content -Raw "include/config/create_usgs_earthquakes.sql" | docker exec -i geocog-postgres psql -U postgres -d earthquakes
```

4. Verify table creation
```powershell
docker exec -it geocog-postgres psql -U postgres -d earthquakes -c "\dt usgs_earthquakes"
```

5. Open Airflow and trigger DAG `usgs_to_minio_daily_http_operator`
   - Airflow: `http://localhost:8080`

6. Verify rows were upserted
```powershell
docker exec -it geocog-postgres psql -U postgres -d earthquakes -c "select count(*) from usgs_earthquakes;"
```

## Running Astro Airflow Locally (Subsequent runs after first time startup)

Astro CLI runs Airflow in its own Docker-managed stack (webserver, scheduler, triggerer, and metadata Postgres).

1. Start Airflow:
```bash
astro dev start
```

2. Then go to `localhost:8080`

3. If restarting Astro:
```bash
astro dev stop
astro dev start
```

4. Hard restarting Astro (Deleting all metadata)

```
astro dev stop
astro dev kill
astro dev start
```

## Reset and Rebuild Local Environment

Use this when you want a clean local restart.

1. Stop and reset Astro containers
```bash
astro dev kill
```

2. Stop and remove Compose services and volumes
```bash
docker compose down -v
```

3. Start again using the first-time startup steps

# Pushing Code

Before pushing code to GitHub, run the following commands

```bash
ruff check . --fix # Finds problems (bugs, bad patterns, lint issues), while fixes what it can
black . # Fixes formatting (style only, not logic)
pytest # Unit testing
```