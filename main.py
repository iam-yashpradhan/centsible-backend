from fastapi import FastAPI, Depends
import psycopg2
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Optional
from decimal import Decimal

load_dotenv()

app = FastAPI()

class CreditRequest(BaseModel):
    merchant_id: int
    user_id: int
    amount: float
    description: Optional[str] = None

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        port=os.getenv("PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("TIMESCALE_DB_PASSWORD")
    )
    return conn

@app.get('/merchants')
def get_users(db_conn=Depends(get_db_connection)):
    cursor = db_conn.cursor()
    cursor.execute("SELECT * FROM merchants")
    users = cursor.fetchall()
    cursor.close()
    db_conn.close()
    return users

@app.post('/merchantCredit')
def credit_user_balance(credit_request: CreditRequest, db_conn=Depends(get_db_connection)):
    cursor = db_conn.cursor()

    
    amount_decimal = Decimal(str(credit_request.amount))

   
    cursor.execute("SELECT balance FROM merchants WHERE merchant_id = %s", (credit_request.merchant_id,))
    merchant = cursor.fetchone()
    
    if not merchant:
        db_conn.close()
        raise HTTPException(status_code=404, detail="Merchant not found")
    
    merchant_balance = Decimal(merchant[0])
    
    if merchant_balance < amount_decimal:
        db_conn.close()
        raise HTTPException(status_code=400, detail="Merchant does not have enough balance")

    cursor.execute("SELECT balance FROM users WHERE user_id = %s", (credit_request.user_id,))
    user = cursor.fetchone()
    
    if not user:
        db_conn.close()
        raise HTTPException(status_code=404, detail="User not found")

    new_user_balance = Decimal(user[0]) + amount_decimal
    cursor.execute("UPDATE users SET balance = %s WHERE user_id = %s", (new_user_balance, credit_request.user_id))

    new_merchant_balance = merchant_balance - amount_decimal
    cursor.execute("UPDATE merchants SET balance = %s WHERE merchant_id = %s", (new_merchant_balance, credit_request.merchant_id))

    
    cursor.execute("""
        INSERT INTO transactions (merchant_id, user_id, amount, status, description)
        VALUES (%s, %s, %s, 'completed', %s)
        """, (credit_request.merchant_id, credit_request.user_id, amount_decimal, credit_request.description))

    db_conn.commit()

    cursor.close()
    db_conn.close()

    return {
        "message": "Balance credited successfully",
        "user_new_balance": str(new_user_balance), 
        "merchant_new_balance": str(new_merchant_balance) 
    }