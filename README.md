# GeoCognition
Data analytics platform for earth data

## Required Installations
 - Docker Desktop
 - Python
 - Astro

## Creating Project

Environment variables

```
conda create -p venv python==3.10 -y
conda activate venv/
pip install -r requirements.txt
```



## Running MinIO Locally

1. Creating MinIO Docker Container:

```bash
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -v ~/minio-data:/data \
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

Restarting Astro:
```bash
astro dev stop
astro dev start
```