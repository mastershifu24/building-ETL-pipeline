"""
Initialize the dimensional model in the data warehouse.

Runs all SQL files from the models/ directory to create
dimension and fact tables.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / '.env', override=True)

from src.utils.database import create_db_engine
from sqlalchemy import text


def init_dimensional_model():
    """
    Execute all SQL model files to create the dimensional schema.
    
    Order matters: dimensions first, then facts (due to foreign keys).
    """
    models_dir = project_root / 'models'
    
    # Order: dimensions first, then facts
    model_order = [
        'dim_date.sql',      # No dependencies
        'dim_plan.sql',      # No dependencies  
        'dim_account.sql',   # No dependencies
        'fact_subscriptions.sql',  # Depends on dim_*
        'fact_user_events.sql',    # Depends on dim_*
    ]
    
    engine = create_db_engine()
    
    print("=" * 60)
    print("Initializing Dimensional Model")
    print("=" * 60)
    
    for filename in model_order:
        filepath = models_dir / filename
        if not filepath.exists():
            print(f"[SKIP] {filename} - not found")
            continue
        
        print(f"\n[RUNNING] {filename}")
        sql_content = filepath.read_text()
        
        # Remove block comments
        import re
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        
        # Split by semicolon to handle multiple statements
        statements = [s.strip() for s in sql_clean.split(';') if s.strip()]
        
        for stmt in statements:
            # Skip empty or comment-only statements
            if not stmt or stmt.startswith('--'):
                continue
            
            # Each statement in its own transaction
            with engine.connect() as conn:
                try:
                    conn.execute(text(stmt))
                    conn.commit()
                except Exception as e:
                    # Ignore "already exists" errors
                    if 'already exists' in str(e).lower():
                        pass
                    elif 'duplicate key' in str(e).lower():
                        pass  # Seed data already inserted
                    else:
                        print(f"  Warning: {str(e)[:80]}")
        
        print(f"  [OK] {filename}")
    
    # Show what tables now exist
    print("\n" + "=" * 60)
    print("Tables in database:")
    print("=" * 60)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name, 
                   (SELECT COUNT(*) FROM information_schema.columns c
                    WHERE c.table_name = t.table_name AND c.table_schema = 'public') as columns
            FROM information_schema.tables t
            WHERE table_schema = 'public'
            ORDER BY table_name
        """))
        
        for row in result:
            # Highlight our dimensional model tables
            prefix = "  * " if row[0].startswith(('dim_', 'fact_')) else "    "
            print(f"{prefix}{row[0]} ({row[1]} columns)")
    
    print("\n[DONE] Dimensional model initialized!")
    print("\nTo explore the tables, connect to PostgreSQL:")
    print("  docker exec -it postgres_warehouse psql -U postgres -d saas_warehouse")
    print("\nThen run: \\dt to list tables, \\d table_name to see columns")


if __name__ == '__main__':
    init_dimensional_model()
