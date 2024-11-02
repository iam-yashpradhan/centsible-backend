
from pydantic import BaseModel
from typing import Optional
from decimal import Decimal

# User Schemas
class UserCreate(BaseModel):
    username: str
    email: str
    phone: Optional[str] = None
    balance: Decimal = Decimal("0.00")

class UserOut(BaseModel):
    user_id: int
    username: str
    email: str
    phone: Optional[str] = None
    balance: Decimal
    created_at: str
    
    class Config:
        from_attributes = True


# Merchant Schemas
class MerchantCreate(BaseModel):
    merchant_name: str
    email: str
    phone: Optional[str] = None
    balance: Decimal = Decimal("0.00")

class MerchantOut(BaseModel):
    merchant_id: int
    merchant_name: str
    email: str
    phone: Optional[str] = None
    balance: Decimal
    created_at: str
    
    class Config:
        from_attributes = True


# Transaction Schemas
class TransactionCreate(BaseModel):
    merchant_id: int
    user_id: int
    amount: Decimal
    transaction_type: str  # 'debit' or 'credit'
    status: str  # 'pending', 'completed', or 'failed'
    description: Optional[str] = None

class TransactionOut(BaseModel):
    transaction_id: int
    merchant_id: int
    user_id: int
    amount: Decimal
    transaction_type: str
    status: str
    created_at: str
    description: Optional[str]

    class Config:
        from_attributes = True  # This replaces orm_mode in Pydantic v2.
