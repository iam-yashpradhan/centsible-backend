# app/main.py

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session

from . import models, schemas
from . import crud
from .database import engine, get_db

app = FastAPI()

models.Base.metadata.create_all(bind=engine)

@app.post("/users/", response_model=schemas.UserOut)
def create_user(user: schemas.UserCreate, db: Session = Depends(get_db)):
    return crud.create_user(db=db, user=user)

@app.post("/merchants/", response_model=schemas.MerchantOut)
def create_merchant(merchant: schemas.MerchantCreate, db: Session = Depends(get_db)):
    return crud.create_merchant(db=db, merchant=merchant)

@app.post("/transactions/", response_model=schemas.TransactionOut)
def create_transaction(transaction: schemas.TransactionCreate, db: Session = Depends(get_db)):
    
    new_transaction = crud.create_transaction(db=db, transaction=transaction)
    
    if not new_transaction:
        raise HTTPException(status_code=400, detail="Transaction failed due to insufficient funds or invalid IDs.")
    
    return new_transaction

@app.get("/users/{user_id}", response_model=schemas.UserOut)
def get_user(user_id: int, db: Session = Depends(get_db)):
    
   user_data = crud.get_user(db=db,user_id=user_id)

   if not user_data:
      raise HTTPException(status_code=404 , detail="User Not Found")
   
   return user_data


@app.get("/merchants/{merchant_id}", response_model=schemas.MerchantOut)
def get_merchant(merchant_id:int ,db :Session=Depends(get_db)):

     merchant_data=crud.get_merchant(db=db ,merchant_id=merchant_id)

     if not merchant_data:
        raise HTTPException(status_code=404 ,detail="Merchant Not Found")

     return merchant_data 
