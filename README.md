# GeoCognition
Data analytics platform for earth data



# Setting Up

## Required Installations
 - Docker Desktop
 - Python
 - Astro

Environment variables

```
conda create -p venv python==3.10 -y
conda activate venv/
pip install -r requirements.txt
```

# Docker Runs (Locally)

## Running MinIO Locally

1. Creating MinIO Docker Container:

```powershell
docker run -d `
  --name minio `
  -p 9000:9000 `
  -p 9001:9001 `
  -v minio-data:/data `
  -e MINIO_ROOT_USER=minio `
  -e MINIO_ROOT_PASSWORD=minio123 `
  minio/minio server /data --console-address ":9001"
```

If you are using Git Bash or WSL instead of PowerShell, use:

```bash
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -v minio-data:/data \
  -e MINIO_ROOT_USER=minio \
  -e MINIO_ROOT_PASSWORD=minio123 \
  minio/minio server /data --console-address ":9001"
```

2. Then go into `localhost:9001`


## Running Astro Airflow Locally

1. If you don't have one, create an `.env` file in the root directory of this project. In the file, add:

```bash
AIRFLOW_CONN_USGS_API=http://earthquake.usgs.gov
```

2. Start Airflow:
```bash
astro dev start
```

Then go to `localhost:8080`

Restarting Astro:
```bash
astro dev stop
astro dev start
```

Hard restarting Astro (Deleting all metadata)

```
astro dev stop
astro dev kill
astro dev start
```

Then go to `localhost:8080`


## Running Postgres Locally

1. Creating Postgres Docker Container:

```powershell
docker run -d `
  --name geocog-postgres `
  -e POSTGRES_USER=postgres `
  -e POSTGRES_PASSWORD=postgres `
  -e POSTGRES_DB=earthquakes `
  -p 5433:5432 `
  -v geocog_pgdata:/var/lib/postgresql/data `
  postgres:16
```

If you are using Git Bash or WSL instead of PowerShell, use:

```bash
docker run -d \
  --name geocog-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=earthquakes \
  -p 5433:5432 \
  -v geocog_pgdata:/var/lib/postgresql/data \
  postgres:16
```