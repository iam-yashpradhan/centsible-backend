from sqlalchemy.orm import Session

from .app import models
from .app import schemas

# User CRUD operations

def create_user(db: Session, user_data: schemas.UserCreate):
    db_user = models.User(**user_data.dict())
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    
    return db_user

def get_user(db: Session, user_id: int):
   return db.query(models.User).filter(models.User.user_id == user_id).first()

def get_all_users(db: Session):
   return db.query(models.User).all()


# Merchant CRUD operations

def create_merchant(db: Session, merchant_data: schemas.MerchantCreate):
   db_merchant = models.Merchant(**merchant_data.dict())
   db.add(db_merchant)
   db.commit()
   db.refresh(db_merchant)

   return db_merchant

def get_merchant(db: Session, merchant_id:int):
   return db.query(models.Merchant).filter(models.Merchant.merchant_id == merchant_id).first()

def get_all_merchants(db :Session):
   return db.query(models.Merchant).all()


# Transaction CRUD operations

def create_transaction(db :Session ,transaction_data :schemas.TransactionCreate):

   # Fetch the user and merchant from the DB to update their balances.
   user=db.query(models.User).filter(models.User.user_id==transaction_data.user_id).first()
   merchant=db.query(models.Merchant).filter(models.Merchant.merchant_id==transaction_data.merchant_id).first()

   if not user or not merchant:
       return None # Handle case where either doesn't exist
   
   # Ensure the transaction type is handled correctly.
   if transaction_data.transaction_type == "debit":
       if merchant.balance < transaction_data.amount:
           return None  # Handle insufficient funds case for debit transactions.
       merchant.balance -= transaction_data.amount  # Deduct from the merchant.
       user.balance += transaction_data.amount      # Add to the user's balance.
   
   elif transaction_data.transaction_type == "credit":
       if user.balance < transaction_data.amount:
           return None  # Handle insufficient funds case for credit transactions.
       user.balance -= transaction_data.amount       # Deduct from the user's balance.
       merchant.balance += transaction_data.amount   # Add to the merchant's balance.

   new_transaction=models.Transaction(**transaction_data.dict())

   db.add(new_transaction)

   # Commit all changes (user & merchant balances + new transaction).
   db.commit()

   return new_transaction


def get_transaction(db :Session ,transaction_id:int):
     return db.query(models.Transaction).filter(models.Transaction.transaction_id == transaction_id).first()

def get_all_transactions(db :Session):
     return db.query(models.Transaction).all()
