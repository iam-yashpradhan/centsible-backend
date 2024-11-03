from fastapi import FastAPI, HTTPException, Depends, Request, Path, Query
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel
from typing import Optional
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Optional
from decimal import Decimal
from datetime import datetime

load_dotenv()

app = FastAPI()

class CreditRequest(BaseModel):
    merchant_id: int
    user_id: int
    amount: float
    description: Optional[str] = None

class User(BaseModel):
    username: str
    email: str
    phone: Optional[str] = None
    balance: float = 0.00

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        port=os.getenv("PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("TIMESCALE_DB_PASSWORD")
    )
    return conn

@app.post('/users')
def handle_user(user: User, db_conn=Depends(get_db_connection)):

    try:
        cursor = db_conn.cursor()
        # Check if user exists
        cursor.execute("SELECT * FROM Users WHERE username=%s OR email=%s", (user.username, user.email))
        existing_user = cursor.fetchone()

        if existing_user:
            print("ship")
            # Update user if it exists
            cursor.execute("""
                UPDATE Users SET balance = %s, created_at = %s WHERE username = %s OR email = %s
            """, (user.balance, datetime.now(), user.username, user.email))
            db_conn.commit()
            return {"message": "User updated"}
        else:
            # Create new user if does not exists
            cursor.execute("""
                INSERT INTO Users (username, email, phone, balance) VALUES (%s, %s, %s, %s)
            """, (user.username, user.email, user.phone, user.balance))
            db_conn.commit()
            return {"message": "User created"}
    except Exception as e:
        db_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        db_conn.close()


@app.get('/users/{user_id}')
def get_user_data(user_id: int = Path(..., title="The ID of the user to retrieve"), db_conn=Depends(get_db_connection)):
    try:
        print(user_id)
        cursor = db_conn.cursor()
        cursor.execute("SELECT * FROM Users WHERE user_id = %s", (user_id,))
        user_data = cursor.fetchone()
        if user_data:
            user = {
                "user_id": user_data[0],
                "username": user_data[1],
                "email": user_data[2],
                "phone": user_data[3],
                "balance": user_data[4],
                "created_at": user_data[5]
            }
            return user
        else:
            raise HTTPException(status_code=404, detail="User not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve user: " + str(e))
    finally:
        cursor.close()
        db_conn.close()
    
    

@app.get('/merchants')
def get_users(db_conn=Depends(get_db_connection)):
    cursor = db_conn.cursor()
    cursor.execute("SELECT * FROM merchants")
    users = cursor.fetchall()
    cursor.close()
    db_conn.close()
    return users

@app.get('/transactions')
def get_transactions(
    user_id: Optional[int] = Query(None),
    merchant_id: Optional[int] = Query(None),
    db_conn=Depends(get_db_connection)
):
    cursor = db_conn.cursor(cursor_factory=RealDictCursor)

    if not user_id and not merchant_id:
        raise HTTPException(status_code=400, detail="Either user_id or merchant_id must be provided")

    if user_id:
        cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        user = cursor.fetchone()
        
        if not user:
            db_conn.close()
            raise HTTPException(status_code=404, detail="User not found")

        cursor.execute("""
            SELECT transaction_id, amount, status, created_at, description 
            FROM transactions 
            WHERE user_id = %s 
            ORDER BY created_at DESC
            """, (user_id,))
        
        transactions = cursor.fetchall()

        if not transactions:
            raise HTTPException(status_code=404, detail="No transactions found for this user")

        response = {
            "entity": "user",
            "details": {
                "user_id": user["user_id"],
                "username": user["username"],
                "email": user["email"],
                "balance": str(user["balance"]),
                "created_at": str(user["created_at"])
            },
            "transactions": transactions
        }

    
    elif merchant_id:
        cursor.execute("SELECT * FROM merchants WHERE merchant_id = %s", (merchant_id,))
        merchant = cursor.fetchone()
        
        if not merchant:
            db_conn.close()
            raise HTTPException(status_code=404, detail="Merchant not found")

        cursor.execute("""
            SELECT transaction_id, amount, status, created_at, description 
            FROM transactions 
            WHERE merchant_id = %s 
            ORDER BY created_at DESC
            """, (merchant_id,))
        
        transactions = cursor.fetchall()

        if not transactions:
            raise HTTPException(status_code=404, detail="No transactions found for this merchant")

        response = {
            "entity": "merchant",
            "details": {
                "merchant_id": merchant["merchant_id"],
                "merchant_name": merchant["merchant_name"],
                "email": merchant["email"],
                "balance": str(merchant["balance"]),
                "created_at": str(merchant["created_at"])
            },
            "transactions": transactions
        }

    cursor.close()
    db_conn.close()

    return response

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



