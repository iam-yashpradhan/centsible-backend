from fastapi import FastAPI, Depends
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        port=os.getenv("PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("TIMESCALE_DB_PASSWORD")
    )
    return conn

@app.get('/users')
def get_users(db_conn=Depends(get_db_connection)):
    cursor = db_conn.cursor()
    cursor.execute("SELECT * FROM merchants")
    users = cursor.fetchall()
    cursor.close()
    db_conn.close()
    return users