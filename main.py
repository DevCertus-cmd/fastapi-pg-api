from fastapi import FastAPI, Query
import psycopg2
import os

app = FastAPI()

@app.get("/get-data")
def get_data(merchant: str = Query(...), date: str = Query(...)):
    start_datetime = f"{date} 00:00:00"
    end_datetime = f"{date} 23:59:59"

    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        port=os.environ.get("DB_PORT", 5432)
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT col1, col2
        FROM stats
        WHERE merchant_name = %s
          AND create_time >= %s
          AND create_time <= %s
        """,
        (merchant, start_datetime, end_datetime)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        return {"col1": row[0], "col2": row[1]}
    else:
        return {"col1": None, "col2": None}

