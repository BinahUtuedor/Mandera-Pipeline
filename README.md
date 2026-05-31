# Mandera Analytics Pipeline

### From Raw Data to Trusted Analytics — A Complete Data Engineering Pipeline

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/airflow-2.9.1-green)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/docker-compose-blue)](https://www.docker.com/)
[![MinIO](https://img.shields.io/badge/minIO-object--storage-red)](https://min.io/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-database-blue)](https://www.postgresql.org/)

## 📌 Overview

The Mandera Analytics Pipeline is an end-to-end data engineering pipeline that moves data from operational systems into analytics-ready platforms. The pipeline implements a modern data architecture using industry-standard tools.

**Technology Stack:**
- **Python** + **Faker** - Synthetic data generation
- **MongoDB Atlas** - Source operational system
- **MinIO** - Data lake storage
- **PostgreSQL** - Data warehousing
- **Pandas** - Data transformation
- **Apache Airflow** - Workflow orchestration
- **Docker** - Containerised environment
- **GitHub Actions** - Automated scheduling of data generation

**Pipeline Capabilities:**
- ✅ Data ingestion from operational systems
- ✅ Data lake storage with partitioning strategies
- ✅ Data warehousing with layered architecture (Raw & Staging)
- ✅ Data transformation and cleansing
- ✅ Workflow orchestration with Apache Airflow
- ✅ Monitoring, validation, and anomaly detection
- ✅ Data documentation and lineage

---

## 🗺️ Architecture Overview

The pipeline follows a modern layered data engineering architecture:

```text
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│    Python       │     │    MongoDB      │     │     MinIO       │
│   Generator     │ ──▶ │    Atlas        │ ──▶ │   Data Lake     │
│    (Faker)      │     │  (Raw Source)   │     │ (Partitioned)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                           │
                                                           ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   PostgreSQL    │ ◀── │     Pandas      │ ◀── │   PostgreSQL    │
│    Staging      │     │   Transform     │     │   Raw Layer     │
│   (Cleansed)    │     │                 │     │  (Immutable)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘

            Apache Airflow Orchestrates All Stages
```

## Data Flow

1. **Data Generation** - Python script generates synthetic transactions

2. **Source Ingestion** - Data stored in MongoDB Atlas

3. **Extract** - Batch extraction from MongoDB to MinIO data lake

4. **Load Raw** - Immutable copy loaded to PostgreSQL raw schema

5. **Validate** - Row count validation and anomaly detection

6. **Transform** - Cleansing and enrichment in staging layer

7. **Monitor** - Batch metrics and error tracking

## 🛠️ Technology Stack

All services except MongoDB Atlas run inside Docker containers. You do not need to install PostgreSQL, MinIO, or pgAdmin/DBeaver natively.

| Tool | Purpose | Version | Installation Method |
|------|---------|---------|---------------------|
| Python + Faker | Generate realistic fake transaction data | 3.9+ | Native (pip) |
| MongoDB Atlas | Store raw source transaction data | Latest | Cloud (SaaS) |
| MinIO | Store partitioned raw files in a data lake | Latest | Docker Container |
| PostgreSQL | Store raw and transformed warehouse data | 15 | Docker Container |
| Pandas | Data cleaning and transformation | Latest | Native (pip) |
| Apache Airflow | Workflow orchestration and monitoring | 2.9.1 | Docker Container |
| Docker | Containerised environment | 20.10+ | Native |
| pgAdmin / DBeaver | Database administration | Latest | Docker Container or Native |

## 📁 Project Structure
```text
mandera-pipeline/
│
├── data_generator/            # Data generation and extraction
│   ├── __init__.py
│   ├── generate_transactions.py
│   └── extract_to_minio.py
│
├── warehouse/                 # Data warehousing
│   ├── __init__.py
│   └── load_to_postgres.py
│
├── transformations/           # Data transformation logic
│   ├── __init__.py
│   └── transform.py
│
├── dags/                      # Airflow DAGs
│   ├── __init__.py
│   └── mandera_pipeline_dag.py
│
├── sql/                       # Database schemas
│   ├── raw_schema.sql
│   └── staging_schema.sql
│
├── scripts/                   # Utility scripts
│   └── airflow-init.sh
│
├── docs/                      # Documentation
│   └── data_dictionary.md
│
├── pgadmin/                   # pgAdmin configuration
│   └── servers.json
│
├── logs/                      # Application logs
├── plugins/                   # Airflow plugins
├── temp/                      # Temporary files
│
├── docker-compose.yml         # Docker services
├── requirements.txt           # Python dependencies
├── .env                       # Environment variables
└── README.md                  # This file
```

🚀 Quick Start

## Prerequisites

Before you begin, ensure you have the following installed:

- [Python 3.9+](https://www.python.org/downloads/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Git](https://git-scm.com/downloads)
- [MongoDB Atlas Account](https://www.mongodb.com/cloud/atlas) (free tier) - accessed via cloud

The following services will run inside Docker containers (no local installation required):
- PostgreSQL
- MinIO
- DBeaver (optional - for database browsing)

All other tools and dependencies will be managed within the Dockerized environment.

## Setup Instructions
### Step 1: Clone the Repository
```bash
git clone https://github.com/your-org/mandera-pipeline.git
cd mandera-pipeline
```
### Step 2: Configure Environment Variables
Create a .env file in the project root:
```env
# Airflow Configuration
AIRFLOW_IMAGE_TAG=2.9.3
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_EMAIL=admin@example.com
FERNET_KEY=<Your fernet key here>

# PostgreSQL Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# MongoDB Atlas (Required - Get from your MongoDB Atlas cluster)
MONGO_URI=mongodb+srv://YOUR_USERNAME:YOUR_PASSWORD@your-cluster.mongodb.net/mandera_pipeline?retryWrites=true&w=majority&tls=true

# MinIO Configuration
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=mandera-lake
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Ports
WEBSERVER_PORT=8080
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
PGADMIN_PORT=5050

# pgAdmin
PGADMIN_EMAIL=admin@mandera.com
PGADMIN_PASSWORD=admin
```
### Step 3: Start Docker Services
```bash
# Start all services in detached mode
docker-compose up -d

# Verify containers are running
docker-compose ps

# Watch the initialization process
docker-compose logs -f airflow-init
```
After your Docker containers are running:
### Step 4: Create Database Schemas
For Windows (PowerShell):
```powershell
Get-Content sql/raw_schema.sql | docker exec -i mandera_postgres psql -U airflow -d airflow
Get-Content sql/staging_schema.sql | docker exec -i mandera_postgres psql -U airflow -d airflow
```
For Linux/Mac:
```bash
docker exec -i mandera_postgres psql -U airflow -d airflow < sql/raw_schema.sql
docker exec -i mandera_postgres psql -U airflow -d airflow < sql/staging_schema.sql
```

### Step 5: Create MinIO Bucket
1. Open MinIO Console: [http://localhost:9001](http://localhost:9001)
2. Login with credentials: `minioadmin` / `minioadmin`
3. Click "Buckets" → "Create Bucket"
4. Name: `mandera-lake`
5. Click "Create Bucket"

### Step 6: Install Python Dependencies
```bash
pip install -r requirements.txt
```
### Step 7: Generate and Load Data
```bash
# Generate synthetic transaction data
python data_generator/generate_transactions.py
# Note the BATCH ID from the output

# Extract MongoDB data to MinIO
python data_generator/extract_to_minio.py

# Load to PostgreSQL raw layer
python warehouse/load_to_postgres.py

# Transform to staging layer
python transformations/transform.py
```

### Step 8: Access Services

| Service | URL | Credentials |
|---------|-----|--------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| pgAdmin | http://localhost:5050 | admin@mandera.com / admin |
| PostgreSQL | localhost:5432 | airflow / airflow |

## 📊 Pipeline Layers

### Raw Layer (`raw` schema)

**Purpose:** Immutable source of truth - exactly as extracted from source system

**Characteristics:**
- ✅ No transformations
- ✅ No cleansing
- ✅ No enrichment
- ✅ Complete recoverability

**Tables:**
- `raw.transactions` - Raw transaction data
- `raw.batch_log` - Batch processing metadata

### Staging Layer (`staging` schema)

**Purpose:** Cleansed, typed, analytics-ready data

**Transformations Applied:**
- Deduplication
- Null handling
- Data type standardisation
- Invalid record isolation
- Derived column creation

**Tables:**
- `staging.transactions` - Cleaned transaction data
- `staging.error_transactions` - Data quality issues

## 🔄 Transformations Detail

### Data Cleaning Operations

| Operation | Description | Implementation |
|-----------|-------------|----------------|
| Remove Duplicates | Deduplicate by transaction_id | `drop_duplicates(subset=['transaction_id'])` |
| Handle NULLs | Fill or flag missing values | `fillna('UNKNOWN')` |
| Standardise Types | Convert to proper data types | `pd.to_datetime()`, `pd.to_numeric()` |
| Filter Invalid | Move bad data to error table | Boolean masking |
| Add Derived Columns | Create analytics columns | `transaction_month`, `amount_category` |

### Amount Categories

| Amount Range | Category |
|--------------|----------|
| < 100 | LOW |
| 100 - 999.99 | MEDIUM |
| ≥ 1000 | HIGH |

## 📈 Monitoring & Validation
Row Count Validation
Each batch is automatically validated after loading:
```python
variance = abs(expected_rows - loaded_rows) / expected_rows
if variance > 0.05:  # 5% threshold
    anomaly_flag = True
```
### Batch Log Schema

| Column | Description |
|--------|-------------|
| batch_id | Unique batch identifier |
| source_record_count | Records in source system |
| loaded_record_count | Records successfully loaded |
| variance_pct | Percentage difference |
| anomaly_flag | True if variance > 5% |
| load_status | SUCCESS / MISMATCH / ANOMALY |
| load_timestamp | When batch was processed |

### Validation Logic
```python
variance = abs(expected_rows - loaded_rows) / expected_rows

if variance > 0.05:
    raise Exception(f"Batch variance exceeded threshold: {variance:.2%}")
elif variance > 0:
    load_status = 'MISMATCH'
else:
    load_status = 'SUCCESS'
```
## 🐳 Docker Services Configuration
All services run inside Docker containers. No native installations required for PostgreSQL, MinIO, or pgAdmin.
```yaml
Services:
  - postgres:      # PostgreSQL database (port 5432)
  - minio:         # Object storage (ports 9000, 9001)
  - pgadmin:       # Database GUI (port 5050)
  - airflow-init:  # One-time Airflow initialisation
  - airflow:       # Airflow webserver & scheduler (port 8080)
```
## Network Configuration
All services run on a shared Docker network (airflow_network) for seamless communication. Service discovery uses container names (e.g., postgres, minio).

## 🔌 Apache Airflow DAG
### DAG Structure
```python
extract_from_mongodb >> upload_to_minio >> load_to_postgres_raw
load_to_postgres_raw >> validate_row_counts
validate_row_counts >> log_batch_metrics
validate_row_counts >> transform_to_staging
```

### DAG Configuration

| Setting | Value |
|---------|-------|
| Schedule | @daily (configurable) |
| Retries | 2 |
| Retry Delay | 5 minutes |
| SLA | 2 hours |
| Catchup | False |
| Email on Failure | Configurable |

### Airflow Features

- ✅ Automatic retries on failure
- ✅ SLA monitoring and alerting
- ✅ Task dependency management
- ✅ Centralised logging
- ✅ Variable and connection management

## 📝 Data Dictionary

### Staging Table Schema

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| transaction_id | VARCHAR(50) | Unique transaction identifier | TXN-BA73318BBF |
| customer_id | VARCHAR(50) | Customer identifier | CUST-1632 |
| customer_name | VARCHAR(100) | Customer full name | Jared Blackwell |
| email | VARCHAR(255) | Customer email address | econley@example.org |
| transaction_date | TIMESTAMP | Date and time of transaction | 2026-02-17 02:00:05 |
| amount | DECIMAL(15,2) | Transaction value | 6627.55 |
| currency | VARCHAR(3) | ISO currency code | USD |
| payment_method | VARCHAR(50) | Payment method | Mobile Money |
| region | VARCHAR(50) | Geographic region | London |
| status | VARCHAR(20) | Transaction status | COMPLETED |
| batch_id | VARCHAR(50) | Source batch identifier | BATCH-676CDB4A |
| transaction_month | VARCHAR(7) | Derived: YYYY-MM format | 2026-02 |
| amount_category | VARCHAR(10) | Derived: LOW/MEDIUM/HIGH | HIGH |

*See docs/data_dictionary.md for complete documentation.*

## ⏱️ Automation with GitHub Actions
### Scheduled Data Generation

```yaml
name: Generate Data
on:
  schedule:
    - cron: '0 */6 * * *'  # Every 6 hours
  workflow_dispatch:        # Manual trigger

jobs:
  generate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Generate Transactions
        run: python data_generator/generate_transactions.py
      - name: Extract to MinIO
        run: python data_generator/extract_to_minio.py
```

## ❗ Common Issues & Solutions
### PowerShell Redirection Error
Error: The '<' operator is reserved for future use

Solution: Use Get-Content with pipe:
```powershell
Get-Content sql/raw_schema.sql | docker exec -i mandera_postgres psql -U airflow -d airflow
```
### MongoDB Connection Error
Error: ConfigurationError: The DNS query name does not exist

Solution: Install dnspython:
```bash
pip install dnspython
```
### Airflow User Creation Fails
```bash
docker exec -it mandera_airflow bash
airflow users delete --username admin
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
exit
docker-compose restart airflow
```
### MinIO Connection Refused
Solution: Ensure MinIO container is healthy:

```bash
docker-compose ps minio
docker-compose logs minio
```
### PostgreSQL Connection Issues
Solution: Verify PostgreSQL is accepting connections:

```bash
docker exec -it mandera_postgres pg_isready -U airflow
```
### pgAdmin Auto-Connection
Create pgadmin/servers.json before building the docker containers the login to pgadmin with password:
```json
{
  "Servers": {
    "1": {
      "Name": "Local Postgres",
      "Group": "Servers",
      "Host": "mandera_postgres",
      "Port": 5432,
      "MaintenanceDB": "airflow",
      "Username": "airflow",
      "Password": "airflow",
      "SSLMode": "prefer"
    }
  }
}
```
### Permission Issues with airflow-init.sh
Solution: Make the script executable:
```bash
chmod +x scripts/airflow-init.sh
```
## 📊 Performance Characteristics

| Component | Typical Performance |
|-----------|---------------------|
| Data Generation | ~200 records/second |
| MongoDB Extraction | ~500 records/second |
| MinIO Upload | ~10 MB/second |
| Raw Load | ~1000 records/second |
| Transformation | ~2000 records/second |

## 🔐 Security Considerations

- All passwords stored in `.env` (excluded from version control)
- MongoDB Atlas uses TLS/SSL encryption
- MinIO supports IAM policies
- Airflow connections encrypted with Fernet key
- Database credentials isolated per service

## 🚀 Pipeline Extensions

Potential extensions to this pipeline:

- **Real-time Streaming** - Add Kafka for streaming ingestion
- **Data Quality** - Integrate Great Expectations
- **Data Lineage** - Add OpenLineage/Marquez
- **Infrastructure as Code** - Terraform for cloud deployment
- **CI/CD Pipeline** - GitHub Actions for automated testing
- **Data Vault** - Implement Data Vault 2.0 architecture
- **dbt Integration** - Replace pandas transformations with dbt

## 📚 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [MongoDB Atlas Documentation](https://docs.mongodb.com/atlas/)
- [MinIO Documentation](https://docs.min.io/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

## 📄 License

MIT License.

## 🤝 Contributing

Contributions welcome!

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 👥 Authors

Mandera Analytics Pipeline Team

## 🙏 Acknowledgements

- Apache Airflow community
- MongoDB Atlas team
- MinIO project
- PostgreSQL developers
- Faker library maintainers

## 📞 Support

For issues:

- Check the Common Issues section
- Review Docker logs: `docker-compose logs [service_name]`
- Open an issue on GitHub
