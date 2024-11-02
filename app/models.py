
from sqlalchemy import Column, Integer, String, Float, DECIMAL, ForeignKey, Text, TIMESTAMP
from sqlalchemy.orm import relationship
from .database import Base
from sqlalchemy.sql import func

class User(Base):
    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    phone = Column(String(20), nullable=True)
    balance = Column(DECIMAL(12, 2), default=0.00)
    created_at = Column(TIMESTAMP, server_default=func.now())

class Merchant(Base):
    __tablename__ = "merchants"

    merchant_id = Column(Integer, primary_key=True, index=True)
    merchant_name = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    phone = Column(String(20), nullable=True)
    balance = Column(DECIMAL(12, 2), default=0.00)
    created_at = Column(TIMESTAMP, server_default=func.now())

class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id = Column(Integer, primary_key=True, index=True)
    merchant_id = Column(Integer, ForeignKey('merchants.merchant_id', ondelete="SET NULL"), nullable=True)
    user_id = Column(Integer, ForeignKey('users.user_id', ondelete="SET NULL"), nullable=True)
    amount = Column(DECIMAL(12, 2), nullable=False)
    transaction_type = Column(String(20), nullable=False)  # 'debit' or 'credit'
    status = Column(String(20), nullable=False)  # 'pending', 'completed', or 'failed'
    created_at = Column(TIMESTAMP, server_default=func.now())
    description = Column(Text, nullable=True)

    user = relationship("User")
    merchant = relationship("Merchant")
