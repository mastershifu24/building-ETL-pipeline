"""
Data models and schemas for the SaaS analytics pipeline.

This module defines the structure of data at each stage of the pipeline.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import json


@dataclass
class UserEvent:
    """
    Schema for user interaction events.
    
    Represents a single user interaction event with metadata and properties.
    """
    event_id: str
    user_id: str
    event_type: str  # Values: page_view, click, feature_used, etc.
    timestamp: datetime
    properties: dict  # JSON-serializable event properties
    session_id: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'event_id': self.event_id,
            'user_id': self.user_id,
            'event_type': self.event_type,
            'timestamp': self.timestamp.isoformat(),
            'properties': json.dumps(self.properties),
            'session_id': self.session_id
        }


@dataclass
class Subscription:
    """Schema for subscription data."""
    subscription_id: str
    user_id: str
    plan_name: str  # free, basic, pro, enterprise
    status: str  # active, cancelled, expired, trial
    start_date: datetime
    end_date: Optional[datetime]
    monthly_revenue: float
    created_at: datetime
    updated_at: datetime
    
    def to_dict(self):
        return {
            'subscription_id': self.subscription_id,
            'user_id': self.user_id,
            'plan_name': self.plan_name,
            'status': self.status,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat() if self.end_date else None,
            'monthly_revenue': self.monthly_revenue,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }


@dataclass
class Transaction:
    """
    Schema for financial transactions.
    
    Represents a financial transaction related to a subscription.
    """
    transaction_id: str
    user_id: str
    subscription_id: str
    amount: float
    currency: str
    transaction_type: str  # Values: payment, refund, upgrade, downgrade
    status: str  # Values: completed, pending, failed
    transaction_date: datetime
    payment_method: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'transaction_id': self.transaction_id,
            'user_id': self.user_id,
            'subscription_id': self.subscription_id,
            'amount': self.amount,
            'currency': self.currency,
            'transaction_type': self.transaction_type,
            'status': self.status,
            'transaction_date': self.transaction_date.isoformat(),
            'payment_method': self.payment_method
        }


@dataclass
class Account:
    """
    Schema for B2B account (company) data.
    
    In B2B SaaS, the account is the paying entity (company),
    while users are people within that company.
    """
    account_id: str
    company_name: str
    industry: str
    company_size: str  # 1-10, 11-50, 51-200, 201-1000, 1000+
    country: str
    signup_date: datetime
    status: str  # active, churned, trial
    
    def to_dict(self) -> dict:
        return {
            'account_id': self.account_id,
            'company_name': self.company_name,
            'industry': self.industry,
            'company_size': self.company_size,
            'country': self.country,
            'signup_date': self.signup_date.isoformat(),
            'status': self.status
        }


@dataclass
class UserProfile:
    """
    Schema for user profile data.
    
    Represents user demographic and account information.
    In B2B, each user belongs to an account (company).
    """
    user_id: str
    account_id: str  # Links user to their company
    email: str
    created_at: datetime
    signup_source: str  # Values: organic, paid, referral
    country: str
    role: Optional[str] = None  # admin, member, viewer
    company_size: Optional[str] = None
    industry: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'user_id': self.user_id,
            'account_id': self.account_id,
            'email': self.email,
            'created_at': self.created_at.isoformat(),
            'signup_source': self.signup_source,
            'country': self.country,
            'role': self.role,
            'company_size': self.company_size,
            'industry': self.industry
        }


# Warehouse table schemas (for PostgreSQL)
WAREHOUSE_SCHEMAS = {
    'accounts': """
        CREATE TABLE IF NOT EXISTS accounts (
            account_id VARCHAR(255) PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL,
            industry VARCHAR(100),
            company_size VARCHAR(50),
            country VARCHAR(100) NOT NULL,
            signup_date TIMESTAMP NOT NULL,
            status VARCHAR(50) NOT NULL,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
        CREATE INDEX IF NOT EXISTS idx_accounts_country ON accounts(country);
    """,
    
    'user_events': """
        CREATE TABLE IF NOT EXISTS user_events (
            event_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            event_type VARCHAR(100) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            properties JSONB,
            session_id VARCHAR(255),
            country VARCHAR(100),
            signup_source VARCHAR(50),
            company_size VARCHAR(50),
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id);
        CREATE INDEX IF NOT EXISTS idx_user_events_timestamp ON user_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_user_events_event_type ON user_events(event_type);
    """,
    
    'subscriptions': """
        CREATE TABLE IF NOT EXISTS subscriptions (
            subscription_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            plan_name VARCHAR(50) NOT NULL,
            status VARCHAR(50) NOT NULL,
            start_date TIMESTAMP NOT NULL,
            end_date TIMESTAMP,
            monthly_revenue DECIMAL(10, 2) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id ON subscriptions(user_id);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON subscriptions(status);
    """,
    
    'transactions': """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            subscription_id VARCHAR(255) NOT NULL,
            amount DECIMAL(10, 2) NOT NULL,
            currency VARCHAR(3) NOT NULL,
            transaction_type VARCHAR(50) NOT NULL,
            status VARCHAR(50) NOT NULL,
            transaction_date TIMESTAMP NOT NULL,
            payment_method VARCHAR(50),
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
        CREATE INDEX IF NOT EXISTS idx_transactions_transaction_date ON transactions(transaction_date);
    """,
    
    'user_profiles': """
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id VARCHAR(255) PRIMARY KEY,
            account_id VARCHAR(255),
            email VARCHAR(255) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            signup_source VARCHAR(50) NOT NULL,
            country VARCHAR(100) NOT NULL,
            role VARCHAR(50),
            company_size VARCHAR(50),
            industry VARCHAR(100),
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_user_profiles_account_id ON user_profiles(account_id);
        CREATE INDEX IF NOT EXISTS idx_user_profiles_country ON user_profiles(country);
        CREATE INDEX IF NOT EXISTS idx_user_profiles_signup_source ON user_profiles(signup_source);
    """
}
