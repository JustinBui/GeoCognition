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
AIRFLOW_CONN_POSTGRES_EARTHQUAKES=postgresql://postgres:postgres@host.docker.internal:5433/earthquakes
AIRFLOW_CONN_POSTGRES_DEFAULT=postgresql://postgres:postgres@host.docker.internal:5433/earthquakes
```

## Running MinIO Locally

1a. Creating MinIO Docker Container:

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

1b. If you are using Git Bash or WSL instead of PowerShell, use:

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

1. Start Airflow:
```bash
astro dev start
```

2. Then go to `localhost:8080`

3. Restarting Astro:
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

## Running Postgres Locally

1a. Creating Postgres Docker Container:

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

1b. If you are using Git Bash or WSL instead of PowerShell, use:

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

2. To run PpostgresSQL inside the container
```
Get-Content -Raw "include/config/create_usgs_earthquakes.sql" | docker exec -i geocog-postgres psql -U postgres -d earthquakes
```

3. Verify table creation
```
docker exec -it geocog-postgres psql -U postgres -d earthquakes -c "\dt usgs_earthquakes"
```