# GeoCognition

Data analytics platform for earth data.

## Stack

- **Airflow** (via Astro CLI) — orchestrates the USGS earthquake data pipeline
- **PostGIS** — stores earthquake data with spatial support
- **MinIO** — object storage for raw and curated data files
- **FastAPI** — REST API serving earthquake data
- **React + Vite** — frontend web app

---

# Local Setup

## Requirements

- Docker Desktop
- Python 3.10
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- Node.js (for the frontend)

## Python Environment

```bash
conda create -p venv python==3.10 -y
conda activate venv/
pip install -r requirements.txt
pip install -r requirements-webapp.txt
```

## Environment Variables

Create a `.env` file in the project root:

```bash
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
GEOCOG_DATABASE_URL=postgresql://postgres:postgres@host.docker.internal:5433/geocog-postgres
AIRFLOW_CONN_USGS_API=http://earthquake.usgs.gov
AIRFLOW_CONN_GEOCOG_POSTGRES_DB=postgresql://postgres:postgres@host.docker.internal:5433/geocog-postgres
```

---

# Running Locally

## 1. Start data services + API (PostGIS, MinIO, FastAPI)

```bash
docker compose up -d
```

Exposes:

- PostGIS on `localhost:5433`
- MinIO API on `localhost:9000`
- MinIO Console on `localhost:9001`
- FastAPI on `localhost:8000`

## 2. Start Airflow

```bash
astro dev start
```

Airflow UI available at `http://localhost:8080`

## 3. Start the frontend (dev mode)

```bash
cd frontend
npm install   # first time only
npm run dev
```

Frontend available at `http://localhost:5173`

---

# API Endpoints

Base URL: `http://localhost:8000`

| Method | Endpoint                 | Description                              |
| ------ | ------------------------ | ---------------------------------------- |
| GET    | `/`                      | Health check                             |
| GET    | `/health`                | Checks database connectivity             |
| GET    | `/earthquakes/years`     | Returns list of years with data          |
| GET    | `/earthquakes?year=2025` | Returns all earthquakes for a given year |

Interactive API docs: `http://localhost:8000/docs`

---

# Stopping Services

## Stop Airflow

```bash
astro dev stop
```

## Stop Docker services

```bash
docker compose down
```

## Full reset (deletes all data)

```bash
astro dev kill
docker compose down -v
```

---

# Before Pushing Code

## Python Code Checks

```bash
# Lint and fix
ruff check . --fix --exclude prototype

# Format
black .

# Run tests (inside Astro bash)
astro dev bash
pytest
```

## React Code Checks

```bash
cd frontend

# Check for lint errors
npm run lint

# Check formatting
npm run format:check

# Auto-fix formatting
npm run format
```
