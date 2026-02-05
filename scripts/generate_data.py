"""
Generate synthetic SaaS company data for testing the pipeline.

This script creates realistic test data for:
- User events
- Subscriptions
- Transactions
- User profiles
"""

import json
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
import random
from pathlib import Path
import uuid

fake = Faker()
Faker.seed(42)
random.seed(42)


def generate_user_profiles(num_users: int = 1000) -> pd.DataFrame:
    """
    Generate synthetic user profile data.
    
    Args:
        num_users: Number of user profiles to generate
        
    Returns:
        DataFrame containing user profile records
    """
    print(f"Generating {num_users} user profiles...")
    
    profiles = []
    for _ in range(num_users):
        profiles.append({
            'user_id': str(uuid.uuid4()),
            'email': fake.email(),
            'created_at': fake.date_time_between(start_date='-2y', end_date='now').isoformat(),
            'signup_source': random.choice(['organic', 'paid', 'referral']),
            'country': fake.country(),
            'company_size': random.choice(['1-10', '11-50', '51-200', '201-1000', '1000+', None]),
            'industry': random.choice(['Technology', 'Finance', 'Healthcare', 'Retail', 'Education', None])
        })
    
    return pd.DataFrame(profiles)


def generate_subscriptions(user_profiles: pd.DataFrame) -> pd.DataFrame:
    """Generate subscription data based on user profiles."""
    print(f"Generating subscriptions for {len(user_profiles)} users...")
    
    plans = {
        'free': 0.0,
        'basic': 29.0,
        'pro': 99.0,
        'enterprise': 299.0
    }
    
    subscriptions = []
    for _, user in user_profiles.iterrows():
        user_id = user['user_id']
        created_at = datetime.fromisoformat(user['created_at'])
        
        # Each user can have subscription history
        num_subscriptions = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
        
        for i in range(num_subscriptions):
            plan_name = random.choice(list(plans.keys()))
            status = random.choice(['active', 'cancelled', 'expired', 'trial'])
            
            start_date = created_at + timedelta(days=random.randint(0, 365))
            
            if status == 'active':
                end_date = None
            elif status == 'trial':
                end_date = start_date + timedelta(days=14)
            else:
                end_date = start_date + timedelta(days=random.randint(30, 365))
            
            subscriptions.append({
                'subscription_id': str(uuid.uuid4()),
                'user_id': user_id,
                'plan_name': plan_name,
                'status': status,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat() if end_date else None,
                'monthly_revenue': plans[plan_name],
                'created_at': start_date.isoformat(),
                'updated_at': (end_date or datetime.now()).isoformat()
            })
    
    return pd.DataFrame(subscriptions)


def generate_transactions(subscriptions: pd.DataFrame) -> pd.DataFrame:
    """
    Generate synthetic transaction data based on active subscriptions.
    
    Creates monthly payment transactions for active paid subscriptions.
    
    Args:
        subscriptions: DataFrame containing subscription data
        
    Returns:
        DataFrame containing transaction records
    """
    print(f"Generating transactions...")
    
    transactions = []
    active_subscriptions = subscriptions[subscriptions['status'] == 'active']
    
    for _, sub in active_subscriptions.iterrows():
        if sub['monthly_revenue'] > 0:
            # Generate monthly payments
            start_date = datetime.fromisoformat(sub['start_date'])
            months_since_start = (datetime.now() - start_date).days // 30
            
            for month in range(min(months_since_start, 12)):  # Max 12 months
                transaction_date = start_date + timedelta(days=month * 30)
                
                transactions.append({
                    'transaction_id': str(uuid.uuid4()),
                    'user_id': sub['user_id'],
                    'subscription_id': sub['subscription_id'],
                    'amount': sub['monthly_revenue'],
                    'currency': 'USD',
                    'transaction_type': 'payment',
                    'status': random.choices(['completed', 'pending', 'failed'], weights=[0.9, 0.08, 0.02])[0],
                    'transaction_date': transaction_date.isoformat(),
                    'payment_method': random.choice(['credit_card', 'paypal', 'bank_transfer'])
                })
    
    return pd.DataFrame(transactions)


def generate_user_events(user_profiles: pd.DataFrame, num_events: int = 10000) -> pd.DataFrame:
    """Generate user event data."""
    print(f"Generating {num_events} user events...")
    
    event_types = [
        'page_view', 'click', 'feature_used', 'signup', 'login',
        'purchase', 'download', 'share', 'comment'
    ]
    
    events = []
    user_ids = user_profiles['user_id'].tolist()
    
    for _ in range(num_events):
        user_id = random.choice(user_ids)
        event_type = random.choice(event_types)
        
        # Generate timestamp within last 90 days
        timestamp = fake.date_time_between(start_date='-90d', end_date='now')
        
        # Generate event properties
        properties = {}
        if event_type == 'page_view':
            properties = {'page': random.choice(['/home', '/pricing', '/features', '/about'])}
        elif event_type == 'feature_used':
            properties = {'feature': random.choice(['dashboard', 'reports', 'api', 'integrations'])}
        elif event_type == 'purchase':
            properties = {'plan': random.choice(['basic', 'pro', 'enterprise'])}
        
        events.append({
            'event_id': str(uuid.uuid4()),
            'user_id': user_id,
            'event_type': event_type,
            'timestamp': timestamp.isoformat(),
            'properties': properties,
            'session_id': str(uuid.uuid4()) if random.random() > 0.3 else None
        })
    
    return pd.DataFrame(events)


def main():
    """
    Generate all synthetic data files for pipeline testing.
    
    Creates user profiles, subscriptions, transactions, and user events
    and saves them as JSON files in the data/raw directory.
    """
    print("=" * 60)
    print("Generating Synthetic SaaS Company Data")
    print("=" * 60)
    
    # Create data directory (relative to project root)
    project_root = Path(__file__).parent.parent
    data_dir = project_root / 'data' / 'raw'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate data
    user_profiles = generate_user_profiles(num_users=1000)
    subscriptions = generate_subscriptions(user_profiles)
    transactions = generate_transactions(subscriptions)
    user_events = generate_user_events(user_profiles, num_events=10000)
    
    # Save to JSON files
    print("\nSaving data files...")
    user_profiles.to_json(data_dir / 'user_profiles.json', orient='records', date_format='iso')
    subscriptions.to_json(data_dir / 'subscriptions.json', orient='records', date_format='iso')
    transactions.to_json(data_dir / 'transactions.json', orient='records', date_format='iso')
    user_events.to_json(data_dir / 'user_events.json', orient='records', date_format='iso')
    
    print(f"\nData generation complete.")
    print(f"\nGenerated files:")
    print(f"  - {data_dir / 'user_profiles.json'}: {len(user_profiles)} records")
    print(f"  - {data_dir / 'subscriptions.json'}: {len(subscriptions)} records")
    print(f"  - {data_dir / 'transactions.json'}: {len(transactions)} records")
    print(f"  - {data_dir / 'user_events.json'}: {len(user_events)} records")


if __name__ == '__main__':
    main()
