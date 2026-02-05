# Production-Style Analytics Pipeline

A production-ready ETL pipeline for a fictional SaaS company, implementing modern data engineering practices.

## Project Overview

This project implements a complete analytics pipeline for a SaaS company, processing user events, subscription data, and business metrics. It includes:

- **ETL/ELT Pipeline**: Extract, Transform, Load processes
- **Data Orchestration**: Workflow management with Apache Airflow
- **Data Warehousing**: Structured data storage and modeling
- **Data Quality**: Validation, monitoring, and error handling
- **Infrastructure as Code**: Docker containerization
- **Production Best Practices**: Logging, monitoring, testing, documentation

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│   Sources   │ --> │  ETL Layer   │ --> │ Data Lake   │ --> │ Data Warehouse│
│  (Simulated)│     │  (Python)    │     │  (S3/Parquet)│     │  (PostgreSQL)│
└─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │ Orchestration│
                    │   (Airflow)  │
                    └──────────────┘
```

## Project Structure

```
.
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .env.example
├── .gitignore
│
├── data/
│   ├── raw/              # Raw extracted data
│   ├── processed/        # Transformed data
│   └── warehouse/        # Final warehouse tables
│
├── src/
│   ├── extract/          # Data extraction modules
│   ├── transform/        # Data transformation logic
│   ├── load/             # Data loading modules
│   ├── models/           # Data models and schemas
│   └── utils/            # Utility functions
│
├── dags/                 # Airflow DAGs
│   └── etl_pipeline.py
│
├── tests/                # Unit and integration tests
│
├── scripts/
│   ├── generate_data.py  # Generate synthetic SaaS data
│   └── setup_db.py       # Database initialization
│
├── monitoring/
│   └── logs/             # Pipeline logs
│
└── docs/                 # Documentation
    ├── architecture.md
    ├── data_models.md
    └── getting_started.md
```

## Quick Start

### Prerequisites

- Python 3.9+
- Docker & Docker Compose
- Git

### Setup

1. **Clone and navigate to the project**
   ```bash
   cd "building ETL pipeline"
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Start services with Docker Compose**
   ```bash
   docker-compose up -d
   ```

6. **Initialize database schema**
   ```bash
   python scripts/setup_db.py
   ```

7. **Generate sample data**
   ```bash
   python scripts/generate_data.py
   ```

8. **Run ETL pipeline**
   ```bash
   python src/main.py
   ```

## Data Sources

- **User Events**: Page views, clicks, feature usage
- **Subscriptions**: Plan changes, renewals, cancellations
- **Transactions**: Payments, refunds, upgrades
- **User Profiles**: Demographics, account creation, updates

## Tech Stack

- **Language**: Python 3.9+
- **Orchestration**: Apache Airflow
- **Data Processing**: Pandas, PySpark (optional)
- **Database**: PostgreSQL
- **Storage**: Local filesystem (S3-compatible for production)
- **Containerization**: Docker
- **Testing**: pytest
- **Monitoring**: Logging, Airflow monitoring

## Features

- Incremental data processing
- Data quality checks and validation
- Error handling and retry logic
- Logging and monitoring
- Scalable architecture
- Documentation and testing

## Technical Implementation

This project implements:
- Production-grade ETL pipeline design
- Data modeling and schema design
- Workflow orchestration
- Error handling and data quality
- Infrastructure setup
- Best practices for data engineering

## License

MIT License

## Contributing

Suggestions and improvements are welcome.
