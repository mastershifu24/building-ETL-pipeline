"""
Generate synthetic SaaS company data for testing the pipeline.

At scale (1M+ events) this script writes user events to Parquet in chunks
so memory stays flat. Smaller datasets still use JSON for quick local runs.
"""

import argparse
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import random

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

EVENT_TYPES = np.array([
    'page_view', 'click', 'feature_used', 'signup', 'login',
    'purchase', 'download', 'share', 'comment'
])
PAGES = np.array(['/home', '/pricing', '/features', '/about'])
FEATURES = np.array(['dashboard', 'reports', 'api', 'integrations'])
PLANS = np.array(['basic', 'pro', 'enterprise'])


def generate_accounts(num_accounts: int = 200) -> pd.DataFrame:
    """Generate synthetic B2B account (company) data."""
    print(f"Generating {num_accounts} B2B accounts...")

    accounts = []
    for _ in range(num_accounts):
        signup_date = fake.date_time_between(start_date='-2y', end_date='now')
        accounts.append({
            'account_id': str(uuid.uuid4()),
            'company_name': fake.company(),
            'industry': random.choice([
                'Technology', 'Finance', 'Healthcare', 'Retail', 'Education', 'Manufacturing'
            ]),
            'company_size': random.choice(['1-10', '11-50', '51-200', '201-1000', '1000+']),
            'country': fake.country(),
            'signup_date': signup_date.isoformat(),
            'status': random.choices(['active', 'churned', 'trial'], weights=[0.7, 0.15, 0.15])[0]
        })

    return pd.DataFrame(accounts)


def generate_user_profiles(
    accounts: pd.DataFrame,
    users_per_account: tuple = (1, 5)
) -> pd.DataFrame:
    """Generate user profiles linked to B2B accounts."""
    print(f"Generating user profiles for {len(accounts)} accounts...")

    profiles = []
    for _, account in accounts.iterrows():
        num_users = random.randint(users_per_account[0], users_per_account[1])
        account_created = datetime.fromisoformat(account['signup_date'])

        for i in range(num_users):
            role = 'admin' if i == 0 else random.choice(['member', 'viewer'])
            user_created = account_created + timedelta(days=random.randint(0, 30))

            profiles.append({
                'user_id': str(uuid.uuid4()),
                'account_id': account['account_id'],
                'email': fake.email(),
                'created_at': user_created.isoformat(),
                'signup_source': random.choice(['organic', 'paid', 'referral']),
                'country': account['country'],
                'role': role,
                'company_size': account['company_size'],
                'industry': account['industry']
            })

    print(f"Generated {len(profiles)} user profiles")
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
        num_subscriptions = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]

        for _ in range(num_subscriptions):
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
    """Generate monthly payment transactions for active paid subscriptions."""
    print("Generating transactions...")

    transactions = []
    active_subscriptions = subscriptions[subscriptions['status'] == 'active']

    for _, sub in active_subscriptions.iterrows():
        if sub['monthly_revenue'] > 0:
            start_date = datetime.fromisoformat(sub['start_date'])
            months_since_start = (datetime.now() - start_date).days // 30

            for month in range(min(months_since_start, 12)):
                transaction_date = start_date + timedelta(days=month * 30)

                transactions.append({
                    'transaction_id': str(uuid.uuid4()),
                    'user_id': sub['user_id'],
                    'subscription_id': sub['subscription_id'],
                    'amount': sub['monthly_revenue'],
                    'currency': 'USD',
                    'transaction_type': 'payment',
                    'status': random.choices(
                        ['completed', 'pending', 'failed'],
                        weights=[0.9, 0.08, 0.02]
                    )[0],
                    'transaction_date': transaction_date.isoformat(),
                    'payment_method': random.choice(['credit_card', 'paypal', 'bank_transfer'])
                })

    return pd.DataFrame(transactions)


def _properties_for_event_type(event_types: np.ndarray) -> list:
    """Build JSON-serializable properties dicts for a batch of event types."""
    properties = []
    for event_type in event_types:
        if event_type == 'page_view':
            properties.append({'page': random.choice(PAGES)})
        elif event_type == 'feature_used':
            properties.append({'feature': random.choice(FEATURES)})
        elif event_type == 'purchase':
            properties.append({'plan': random.choice(PLANS)})
        else:
            properties.append({})
    return properties


def _generate_event_chunk(
    user_ids: np.ndarray,
    chunk_size: int,
    base_time: datetime
) -> pd.DataFrame:
    """Generate one chunk of user events using vectorized random selection."""
    sampled_user_ids = np.random.choice(user_ids, size=chunk_size)
    event_type_indices = np.random.randint(0, len(EVENT_TYPES), size=chunk_size)
    event_types = EVENT_TYPES[event_type_indices]

    # Random timestamps within the last 90 days
    seconds_ago = np.random.randint(0, 90 * 24 * 3600, size=chunk_size)
    timestamps = [base_time - timedelta(seconds=int(s)) for s in seconds_ago]

    session_mask = np.random.random(chunk_size) > 0.3
    session_ids = [
        str(uuid.uuid4()) if has_session else None
        for has_session in session_mask
    ]

    return pd.DataFrame({
        'event_id': [str(uuid.uuid4()) for _ in range(chunk_size)],
        'user_id': sampled_user_ids,
        'event_type': event_types,
        'timestamp': [ts.isoformat() for ts in timestamps],
        'properties': _properties_for_event_type(event_types),
        'session_id': session_ids
    })


def generate_user_events(
    user_profiles: pd.DataFrame,
    num_events: int = 1_000_000,
    chunk_size: int = 50_000
) -> pd.DataFrame:
    """
    Generate user events in memory (for smaller datasets).

    For large datasets, use write_user_events_parquet instead.
    """
    print(f"Generating {num_events:,} user events in memory...")

    user_ids = user_profiles['user_id'].to_numpy()
    base_time = datetime.now()
    chunks = []

    for offset in range(0, num_events, chunk_size):
        n = min(chunk_size, num_events - offset)
        chunks.append(_generate_event_chunk(user_ids, n, base_time))
        print(f"  generated {min(offset + n, num_events):,} / {num_events:,}")

    return pd.concat(chunks, ignore_index=True)


def write_user_events_parquet(
    user_profiles: pd.DataFrame,
    output_path: Path,
    num_events: int = 1_000_000,
    chunk_size: int = 50_000
) -> int:
    """
    Stream user events to Parquet in chunks to keep memory usage flat.

    Returns:
        Total number of events written.
    """
    print(f"Writing {num_events:,} user events to {output_path} (chunk_size={chunk_size:,})...")

    user_ids = user_profiles['user_id'].to_numpy()
    base_time = datetime.now()
    writer = None
    rows_written = 0

    try:
        output_path.unlink(missing_ok=True)

        for offset in range(0, num_events, chunk_size):
            n = min(chunk_size, num_events - offset)
            chunk_df = _generate_event_chunk(user_ids, n, base_time)
            # JSON strings keep a stable Parquet schema across chunks (dict keys vary by event type)
            chunk_df['properties'] = chunk_df['properties'].apply(json.dumps)
            table = pa.Table.from_pandas(chunk_df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema, compression='snappy')
            writer.write_table(table)

            rows_written += n
            print(f"  wrote {rows_written:,} / {num_events:,}")
    finally:
        if writer is not None:
            writer.close()

    return rows_written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Generate synthetic SaaS pipeline data')
    parser.add_argument(
        '--events',
        type=int,
        default=int(__import__('os').getenv('NUM_EVENTS', '1000000')),
        help='Number of user events to generate (default: 1,000,000)'
    )
    parser.add_argument(
        '--accounts',
        type=int,
        default=200,
        help='Number of B2B accounts (default: 200)'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=int(__import__('os').getenv('ETL_CHUNK_SIZE', '50000')),
        help='Rows per chunk when writing large event files (default: 50,000)'
    )
    parser.add_argument(
        '--small',
        action='store_true',
        help='Quick dev run: 10,000 events as JSON instead of 1M Parquet'
    )
    return parser.parse_args()


def main():
    args = parse_args()
    num_events = 10_000 if args.small else args.events

    print("=" * 60)
    print("Generating Synthetic B2B SaaS Company Data")
    print("=" * 60)
    print(f"Target events: {num_events:,}")

    project_root = Path(__file__).parent.parent
    data_dir = project_root / 'data' / 'raw'
    data_dir.mkdir(parents=True, exist_ok=True)

    accounts = generate_accounts(num_accounts=args.accounts)
    user_profiles = generate_user_profiles(accounts, users_per_account=(1, 5))
    subscriptions = generate_subscriptions(user_profiles)
    transactions = generate_transactions(subscriptions)

    print("\nSaving dimension-style files (JSON)...")
    accounts.to_json(data_dir / 'accounts.json', orient='records', date_format='iso')
    user_profiles.to_json(data_dir / 'user_profiles.json', orient='records', date_format='iso')
    subscriptions.to_json(data_dir / 'subscriptions.json', orient='records', date_format='iso')
    transactions.to_json(data_dir / 'transactions.json', orient='records', date_format='iso')

    events_path_json = data_dir / 'user_events.json'
    events_path_parquet = data_dir / 'user_events.parquet'

    if num_events >= 100_000:
        if events_path_json.exists():
            events_path_json.unlink()
        rows_written = write_user_events_parquet(
            user_profiles,
            events_path_parquet,
            num_events=num_events,
            chunk_size=args.chunk_size
        )
        events_label = f"{events_path_parquet} ({rows_written:,} events, Parquet)"
    else:
        if events_path_parquet.exists():
            events_path_parquet.unlink()
        user_events = generate_user_events(
            user_profiles,
            num_events=num_events,
            chunk_size=args.chunk_size
        )
        user_events.to_json(events_path_json, orient='records', date_format='iso')
        events_label = f"{events_path_json} ({len(user_events):,} events, JSON)"

    print("\nData generation complete.")
    print("\nGenerated files:")
    print(f"  - {data_dir / 'accounts.json'}: {len(accounts):,} B2B accounts")
    print(f"  - {data_dir / 'user_profiles.json'}: {len(user_profiles):,} users")
    print(f"  - {data_dir / 'subscriptions.json'}: {len(subscriptions):,} subscriptions")
    print(f"  - {data_dir / 'transactions.json'}: {len(transactions):,} transactions")
    print(f"  - {events_label}")


if __name__ == '__main__':
    main()
