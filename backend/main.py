import os

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query

app = FastAPI()


def get_db_connection():
    return psycopg2.connect(os.getenv("GEOCOG_DATABASE_URL"), connect_timeout=3)


@app.get("/")
def home():
    return {"message": "GeoCognition API is running"}


@app.get("/health")
def health():
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "ok", "database": "connected"}
    except Exception as exc:
        return {"status": "error", "database": str(exc)}


@app.get("/earthquakes/years")
def get_available_years():
    """Returns the list of years that have earthquake data."""
    sql = """
        SELECT DISTINCT EXTRACT(YEAR FROM to_timestamp(time / 1000))::int AS year
        FROM usgs_earthquakes
        ORDER BY year DESC
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        conn.close()
        return {"years": [row[0] for row in rows]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/earthquakes")
def get_earthquakes(year: int = Query(..., description="Filter earthquakes by year")):
    """Returns earthquakes for a given year."""
    sql = """
        SELECT id, mag, place, time, latitude, longitude, depth_km, title
        FROM usgs_earthquakes
        WHERE EXTRACT(YEAR FROM to_timestamp(time / 1000)) = %s
        ORDER BY time DESC
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (year,))
            rows = cur.fetchall()
        conn.close()
        return {"year": year, "count": len(rows), "earthquakes": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
