import os

import psycopg2
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def home():
    return {"message": "GeoCognition API is running"}


@app.get("/health")
def health():
    try:
        conn = psycopg2.connect(os.getenv("GEOCOG_DATABASE_URL"), connect_timeout=3)
        conn.close()
        return {"status": "ok", "database": "connected"}
    except Exception as exc:
        return {"status": "error", "database": str(exc)}
