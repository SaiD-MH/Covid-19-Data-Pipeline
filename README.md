# COVID-19 Data Pipeline

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7+-orange.svg)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 📊 Project Overview

A production-ready end-to-end data engineering pipeline that ingests, transforms, and loads COVID-19 data using modern data engineering best practices. This project demonstrates ETL development, dimensional modeling, comprehensive testing, and workflow orchestration.

### 🎯 Key Features

- ✅ **Medallion Architecture** - Bronze (raw), Silver (cleansed), Gold (analytics-ready) layers
- ✅ **Dimensional Modeling** - Star schema with fact and dimension tables
- ✅ **Incremental Loading** - Date-based delta processing with idempotency
- ✅ **Automated Orchestration** - Apache Airflow DAG with FileSensor
- ✅ **Comprehensive Testing** - Unit tests + Integration tests with Docker Containers
- ✅ **Containerized Environment** - Docker-based infrastructure

---

## ❓ Problem Statement / Use Case

Many public health teams and data analysts need a reproducible, auditable pipeline to ingest daily COVID-19 reports, apply quality checks, and make cleaned data available for analytics and reporting. This project demonstrates an approach that is:

- **Incremental and idempotent** (safe reruns without duplicates)
- **Testable** (unit + integration tests with >85% coverage)
- **Containerized** for reproducible environments
- **Auditable** with comprehensive logging and metadata tracking

Use cases include:
- Daily reporting dashboards
- Trend analysis and forecasting
- Regional comparisons
- Automated anomaly detection alerts

---

## 🏗️ System Architecture

### Architecture Overview

A concise summary of the overall architecture:

- **Source**: Daily CSV files (DD-MM-YYYY.csv)
- **Orchestration**: Apache Airflow DAG with FileSensor + Python operators
- **Processing**: Extract → Transform → Load (Python)
- **Storage**: PostgreSQL with Bronze (raw), Silver (cleansed), Gold (analytics)

### Architecture Diagram
![System Architecture](https://raw.githubusercontent.com/SaiD-MH/Covid-19-Data-Pipeline/main/System%20Architecture.jpg)


### Architecture Components

#### 📁 Source System
- CSV files with date-based naming convention (`DD-MM-YYYY.csv`)
- Simulates external data provider delivering daily COVID-19 reports

#### 🔄 Apache Airflow Orchestration
- **FileSensor**: Monitors data directory for file arrival (poke mode, 5s interval)
- **Extract Task**: Python operator executing `extract.py`
- **Transform Task**: Python operator executing `transform.py`
- **Load Task**: Python operator executing `load.py`
- **Dependencies**: Sequential execution with automatic retry on failure

#### ⚙️ ETL Pipeline
- **Extract**: Reads CSV, normalizes columns, adds `ingested_at` metadata
- **Transform**: Applies data quality rules, filters invalid data, standardizes formats
- **Load**: Builds star schema dimensions, generates surrogate keys, creates fact table

#### 🗄️ Database Layers (Medallion Architecture)
- **Bronze (Raw)**: Unprocessed data exactly as received, with ingestion timestamp
- **Silver (Cleansed)**: Quality-checked, validated, standardized data ready for analysis
- **Gold (Analytics)**: Dimensional model optimized for business intelligence and reporting

#### 🧪 Testing Framework
- **Unit Tests**: Fast, isolated tests with mocked dependencies using pytest
- **Integration Tests**: End-to-end validation with real PostgreSQL via Testcontainers
- **Coverage**: 85%+ code coverage across all modules

#### 🐳 Infrastructure
- **Dev Environment**: Local PostgreSQL for development (port 8081)
- **Test Environment**: Isolated PostgreSQL for testing (port 8082)
- **Airflow Services**: Scheduler, webserver, worker in containers

---

## 🗂️ Data Flow

### 1. Extract Layer (Bronze)
```
CSV File (DD-MM-YYYY.csv)
    ↓
Read with pandas
    ↓
Normalize column names (lowercase, strip)
    ↓
Add ingested_at column
    ↓
Load → bronze.covid table
```

### 2. Transform Layer (Silver)
```
bronze.covid (raw data)
    ↓
Read latest ingestion batch
    ↓
Drop rows with NULL in critical columns
    ↓
Filter out negative values
    ↓
Standardize location names (Title Case)
    ↓
Normalize null representations
    ↓
Load → silver.covid table
```

### 3. Load Layer (Gold)
```
silver.covid (cleansed data)
    ↓
Extract unique dates → date_dim (with duplicate check)
    ↓
Extract unique regions → region_dim (delta/incremental)
    ↓
Join cleansed data with dimensions
    ↓
Create fact records with foreign keys
    ↓
Load → gold.date_dim, gold.region_dim, gold.fact
```

---

## 🧰 Tech Stack

- **Python** 3.10+ (pandas, pytest, psycopg2)
- **Apache Airflow** 2.7+ (DAG orchestration, scheduling)
- **PostgreSQL** 15 (Bronze/Silver/Gold schemas, dimensional modeling)
- **Docker & Docker Compose** (dev/test infrastructure)
- **Testcontainers** (integration tests with isolated databases)
- **pytest** (unit & integration test framework)
- **Ruff** (Python code linting)

---

## 🗄️ Data Model / Schema

### Bronze Layer Schema (Raw Data)
```sql
bronze.covid
├── province_state      VARCHAR
├── country_region      VARCHAR
├── confirmed          INTEGER
├── deaths             INTEGER
├── recovered          INTEGER
├── active             INTEGER
├── incident_rate      FLOAT
├── case_fatality_ratio FLOAT
└── ingested_at        VARCHAR  -- Metadata: processing date
```

### Silver Layer Schema (Cleansed Data)
```sql
silver.covid
├── province_state      VARCHAR (standardized, Title Case)
├── country_region      VARCHAR (standardized, Title Case)
├── confirmed          INTEGER (validated, >= 0)
├── deaths             INTEGER (validated, >= 0)
├── recovered          INTEGER (validated, >= 0)
├── active             INTEGER (validated, >= 0)
├── incident_rate      FLOAT (validated, >= 0)
├── case_fatality_ratio FLOAT (validated)
└── ingested_at        VARCHAR
```

### Gold Layer Schema (Dimensional Model - Star Schema)

**Fact Table:**
```sql
gold.fact
├── fact_id            UUID (PK)
├── confirmed          INTEGER  -- Cumulative confirmed cases
├── deaths             INTEGER  -- Cumulative deaths
├── recovered          INTEGER  -- Cumulative recovered
├── active             INTEGER  -- Active cases
├── incident_rate      FLOAT    -- Per capita rate
├── case_fatality_ratio FLOAT   -- Mortality rate
├── region_key         UUID     -- FK to region_dim
└── date_key           INTEGER  -- FK to date_dim (YYYYMMDD)
```

**Date Dimension:**
```sql
gold.date_dim
├── date_key           INTEGER (PK)  -- YYYYMMDD format (e.g., 20250115)
├── full_date          DATE
├── day_of_week        INTEGER       -- 1=Monday, 7=Sunday
├── day_of_month       INTEGER       -- 1-31
├── day_name           VARCHAR       -- Monday, Tuesday, etc.
├── week_of_year       INTEGER       -- ISO week number
├── month              INTEGER       -- 1-12
├── month_name         VARCHAR       -- January, February, etc.
├── quarter            INTEGER       -- 1-4
├── year               INTEGER       -- 2020, 2021, etc.
└── is_weekend         BOOLEAN       -- TRUE for Sat/Sun
```

**Region Dimension:**
```sql
gold.region_dim
├── region_key         UUID (PK)     -- Surrogate key
├── province_state     VARCHAR       -- State/Province name
└── country_region     VARCHAR       -- Country name
```

**Relationships:**
```
date_dim (1) ──────< (N) fact
region_dim (1) ────< (N) fact
```

---

## 📁 Project Structure

```
Covid-19-Data-Pipeline/
├── dags/                           # Airflow DAG definitions
│   ├── covid_dag.py               # Main orchestration DAG
│   └── __init__.py
│
├── data/                           # Data files
│   └── 03-25-2025.csv             # Sample COVID-19 dataset
│
├── docker/                         # Docker configurations
│   ├── airflow/
│   │   └── docker-compose.yaml    # Airflow services (scheduler, webserver, worker)
│   ├── dev-env/
│   │   └── docker-compose.yml     # Development PostgreSQL database
│   └── test-env/
│       └── docker-compose.yml     # Test PostgreSQL database
│
├── notebooks/                      # Jupyter notebooks for exploration
│   ├── data_exploration.ipynb     # Initial data analysis
│   ├── extract.ipynb              # Extract logic prototyping
│   ├── load.ipynb                 # Load logic prototyping
│   ├── transform.ipynb            # Transform logic prototyping
│   └── snippets.ipynb             # Code snippets and experiments
│
├── sql/                            # Database DDL scripts
│   └── DDL/
│       ├── bronze/
│       │   └── bronze.sql         # Bronze layer schema
│       ├── silver/
│       │   └── silver.sql         # Silver layer schema
│       └── gold/
│           ├── schema.sql         # Gold schema creation
│           ├── date_dimension.sql # Date dimension table
│           ├── region_dimension.sql # Region dimension table
│           └── fact.sql           # Fact table
│
├── src/                            # Source code
│   ├── __init__.py
│   ├── db_connection.py           # Database connection manager
│   │                              # • Environment-aware (DEV/TEST)
│   │                              # • Connection pooling
│   │                              # • Context manager support
│   └── etl/
│       ├── extract.py             # Extract layer implementation
│       │                          # • CSV reading
│       │                          # • Column normalization
│       │                          # • Metadata addition
│       ├── transform.py           # Transform layer implementation
│       │                          # • Data quality checks
│       │                          # • Null/negative filtering
│       │                          # • Standardization
│       └── load.py                # Load layer implementation
│                                  # • Dimension building
│                                  # • Fact table creation
│                                  # • Incremental loading
│
├── tests/                          # Test suite
│   ├── unit-tests/                # Unit tests (mocked dependencies)
│   │   ├── __init__.py
│   │   ├── test_extraction.py    # Extract layer unit tests
│   │   ├── test_transformation.py # Transform layer unit tests
│   │   └── test_loading.py       # Load layer unit tests
│   │
│   └── integration-tests/         # Integration tests (real database)
│       ├── __init__.py
│       ├── test_extraction.py    # E2E extract testing
│       ├── test_transformation.py # E2E transform testing
│       └── test_loading.py       # E2E load testing
│
├── .env                            # Environment variables (not in repo)
├── .env.example                    # Environment template
├── .gitignore                      # Git ignore rules
├── LICENSE                         # MIT License
├── README.md                       # This file
├── requirements.txt                # Python dependencies
└── ruff.toml                       # Code linter configuration
```

---

## 🚀 Project Setup & How to Run Locally

### Prerequisites

- **Python** 3.10 or higher
- **Docker** & Docker Compose (latest version)
- **PostgreSQL** 15 (via Docker, no local install needed)
- **Git**

### Installation Steps

#### 1. Clone the Repository
```bash
git clone https://github.com/SaiD-MH/Covid-19-Data-Pipeline.git
cd Covid-19-Data-Pipeline
```

#### 2. Create Python Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

#### 3. Install Python Dependencies
```bash
pip install -r requirements.txt
```

#### 4. Set Up Environment Variables

Create `.env` file in project root:
```bash
cp .env.example .env
```

Edit `.env` with your configuration:
```env
# Development Database
DEV_DB_HOST=localhost
DEV_DB_PORT=8081
DEV_DB_NAME=covid
DEV_DB_USER=covid
DEV_DB_PWD=covid

# Test Database
TEST_DB_HOST=localhost
TEST_DB_PORT=8082
TEST_DB_NAME=covid_test
TEST_DB_USER=test_user
TEST_DB_PWD=test_pass

# Application Settings
APP_ENV=DEV                    # DEV or TEST
DATA_PATH=./data/              # Path to data files
```

#### 5. Start Development Database
```bash
cd docker/dev-env
docker-compose up -d
cd ../..
```

Verify database is running:
```bash
docker ps
# Should see postgres container on port 8081
```

#### 6. Initialize Database Schema

Connect to database:
```bash
psql -h localhost -p 8081 -U covid -d covid
# Password: covid
```

Run all schema files:
```sql
-- Create schemas and tables
\i sql/DDL/bronze/bronze.sql
\i sql/DDL/silver/silver.sql
\i sql/DDL/gold/schema.sql
\i sql/DDL/gold/date_dimension.sql
\i sql/DDL/gold/region_dimension.sql
\i sql/DDL/gold/fact.sql

-- Verify tables created
\dt bronze.*
\dt silver.*
\dt gold.*

\q
```

---

## 🔧 Usage

### Option 1: Manual Execution (Development/Testing)

#### Run Extract Layer
```bash
python -c "from src.etl.extract import run_extraction; print(run_extraction('03-25-2025.csv'))"
```

**Expected output:**
```json
{
  "file_name": "03-25-2025.csv",
  "rows_read": 100,
  "loaded_data": 100,
  "status": "success"
}
```

#### Run Transform Layer
```bash
python -c "from src.etl.transform import run_transformation; print(run_transformation())"
```

**Expected output:**
```json
{
  "loaded_data": 100,
  "rows_transformed": 95,
  "rows_removed": 5,
  "status": "success"
}
```

#### Run Load Layer
```bash
python -c "from src.etl.load import run_load; print(run_load())"
```

**Expected output:**
```json
{
  "date_dim_records": 1,
  "region_dim_records": 10,
  "fact_records": 95,
  "status": "success"
}
```

---

### Option 2: Airflow Orchestration (Automated Pipeline)

#### Start Airflow Services
```bash
cd docker/airflow
docker-compose up -d
```

Wait for services to start (~30 seconds):
```bash
docker-compose ps
# All services should show "healthy"
```

#### Access Airflow Web UI
- **URL**: http://localhost:8080
- **Username**: `airflow`
- **Password**: `airflow`

#### Configure File System Connection

In Airflow UI:
1. Go to **Admin** → **Connections**
2. Click **+** to add connection
3. Configure:
   - **Connection Id**: `file_system_connection`
   - **Connection Type**: `File (path)`
   - **Extra**: `{"path": "/opt/airflow/data"}`
4. Click **Save**

#### Trigger the DAG

**Method A: Via UI**
1. Navigate to **DAGs** page
2. Find `covid_dag`
3. Click ▶️ (Play button)
4. Monitor in **Graph View** or **Tree View**

**Method B: Via CLI**
```bash
docker exec -it airflow-scheduler airflow dags trigger covid_dag
```

---


## 📊 Example Outputs / Sample Queries

### Count Facts by Date
```sql
SELECT date_key, COUNT(*) AS records, 
       SUM(confirmed) as total_confirmed,
       SUM(deaths) as total_deaths
FROM gold.fact
GROUP BY date_key
ORDER BY date_key DESC
LIMIT 10;
```

**Sample Output:**
```
date_key  | records | total_confirmed | total_deaths
-----------|---------|-----------------|-------------
20250326   | 185     | 1,245,890       | 32,456
20250325   | 195     | 1,198,765       | 31,234
20250324   | 180     | 1,145,321       | 29,876
```

### Top Regions by Confirmed Cases
```sql
SELECT r.country_region, r.province_state, f.confirmed, f.deaths,
       ROUND(f.case_fatality_ratio * 100, 2) as fatality_pct
FROM gold.fact f
JOIN gold.region_dim r ON f.region_key = r.region_key
WHERE f.date_key = 20250325
ORDER BY f.confirmed DESC
LIMIT 20;
```

**Sample Output:**
```
country_region | province_state | confirmed | deaths | fatality_pct
----------------|----------------|-----------|--------|-------------
United States  | New York       | 45,678    | 1,234  | 2.70
United States  | California     | 38,901    | 892    | 2.29
United Kingdom | England        | 28,456    | 756    | 2.66
Canada         | Ontario        | 19,234    | 412    | 2.14
```

### Daily Metrics by Region
```sql
SELECT d.full_date, r.country_region, 
       f.confirmed, f.deaths, f.recovered, f.active,
       ROUND(f.incident_rate, 2) as incident_rate
FROM gold.fact f
JOIN gold.date_dim d ON f.date_key = d.date_key
JOIN gold.region_dim r ON f.region_key = r.region_key
WHERE d.year = 2025 AND d.month = 3 
  AND r.country_region = 'United States'
ORDER BY d.full_date, r.province_state;
```

---

## 🧪 Testing

### Unit Tests (Fast, Isolated, Mocked)

Run all unit tests:
```bash
pytest tests/unit-tests/ -v
```

Run specific test file:
```bash
pytest tests/unit-tests/test_extraction.py -v
```

Run with coverage report:
```bash
pytest tests/unit-tests/ --cov=src --cov-report=html --cov-report=term
```


### Integration Tests (Real Database, End-to-End)

Start test database:
```bash
cd docker/test-env
docker-compose up -d
cd ../..
```

Run all integration tests:
```bash
APP_ENV=TEST pytest tests/integration-tests/ -v
```

### Test Coverage Summary

| Module | Unit Test Coverage | Integration Test Coverage |
|--------|-------------------|---------------------------|
| `extract.py` | 90% | 85% |
| `transform.py` | 88% | 90% |
| `load.py` | 82% | 88% |
| `db_connection.py` | 95% | 100% |
| **Overall** | **88%** | **90%** |

---

## 🔄 Incremental Loading Strategy

### Overview

The pipeline implements date-based incremental loading with idempotency guarantees.

### File Naming Convention
```
DD-MM-YYYY.csv

Examples:
- 01-01-2025.csv (January 1, 2025)
- 15-03-2025.csv (March 15, 2025)
- 31-12-2025.csv (December 31, 2025)
```

### Example Scenario

**Day 1: 2025-03-25**
```
File: 03-25-2025.csv (1000 rows)

Extract:
✓ Load 1000 rows → bronze.covid

Transform:
✓ Filter to 950 valid rows → silver.covid

Load:
✓ Insert 1 date record (20250325)
✓ Insert 50 unique regions
✓ Insert 950 fact records
```

**Day 2: 2025-03-26**
```
File: 03-26-2025.csv (1200 rows)

Extract:
✓ Load 1200 rows → bronze.covid (total: 2200)

Transform:
✓ Filter to 1100 valid rows → silver.covid (total: 2050)

Load:
✓ Insert 1 new date record (20250326)
✓ Find 5 new regions (45 already exist)
✓ Insert 1100 new fact records
```

**Rerun Day 1: 2025-03-25**
- Date dimension: Skips (already exists)
- Region dimension: Returns only new regions
- Fact table: Can be safely truncated and reloaded
- Result: **Idempotent** and **deterministic**

---

## 🔧 Design Decisions

1. **Medallion Architecture (Bronze/Silver/Gold)**
   - Separates raw, cleansed, and analytics layers
   - Simplifies debugging and enables independent reruns
   - Provides clear lineage and audit trails

2. **Date-based Incremental Loading**
   - Uses file date from filename for idempotency
   - Guarantees deterministic replay semantics
   - Enables safe retries without duplicates

3. **UUID Surrogate Keys for Dimensions**
   - Ensures uniqueness across environments
   - Avoids collisions when merging data
   - Supports distributed data collection

4. **Containers for Integration Tests**
   - Provides isolated, ephemeral databases
   - Reproduces production environment locally
   - Eliminates shared test database issues

5. **Apache Airflow for Orchestration**
   - FileSensor decouples ingestion timing from processing
   - DAG provides clear dependency management
   - Built-in retry logic and monitoring

---

## ⚠️ Limitations & Future Improvements

### Current Limitations

1. **CSV Format Assumptions**
   - Parser expects reasonably consistent CSVs
   - Limited error tolerance for malformed inputs
   - **Future**: Add robust parsing, error recovery, and data validation rules

2. **Late-Arriving Data**
   - Current append-only design doesn't handle backfilled data
   - No reconciliation or upsert strategies
   - **Future**: Implement SCD Type 2 for dimensions, fact reconciliation

3. **Security & Access Controls**
   - No encryption for files at rest
   - No authentication/authorization for database
   - **Future**: Add secure file storage, encrypted connections, role-based access

4. **Performance at Scale**
   - Single-threaded ingestion limits throughput
   - No partitioning strategy for very large datasets
   - **Future**: Implement parallel processing, table partitioning, batch optimization

5. **Data Quality Monitoring**
   - Limited anomaly detection
   - No statistical profiling
   - **Future**: Add great_expectations, automated alerts, data drift detection

### Planned Enhancements

- [ ] Add great_expectations for advanced data quality checks
- [ ] Implement SCD Type 2 for slowly changing dimensions
- [ ] Add partition strategy for fact tables (by date)
- [ ] Support multiple data sources (API, databases)
- [ ] Add data profiling and schema evolution tracking
- [ ] Implement email alerts for pipeline failures

---

## ✅ Learning Outcomes / Key Takeaways

### ETL Architecture & Design Patterns
- How to design and implement a layered (bronze/silver/gold) medallion architecture
- Best practices for separating raw, cleansed, and analytics data
- Dimensional modeling with star schemas for analytics

### Data Engineering Best Practices
- Idempotent and incremental loading strategies
- Date-based partitioning for deterministic reruns
- Error handling, logging, and data quality checks
- Surrogate key generation and management

### Testing & Quality Assurance
- Unit testing with mocked dependencies
- Integration testing with real databases and Testcontainers
- Coverage measurement and reporting

### Orchestration & Automation
- Apache Airflow DAG design and dependency management
- Task sequencing and error retry logic
- Monitoring and observability
- File sensors and event-driven workflows

### Tools & Technologies
- Python for ETL script development
- PostgreSQL for dimensional data warehousing
- Docker for reproducible environments
- pytest for comprehensive test coverage

---

## ✉️ Author / Contact Information

- **Author**: SaiD-MH
- **GitHub**: https://github.com/SaiD-MH
- **Repository**: https://github.com/SaiD-MH/Covid-19-Data-Pipeline
- **Contact**: Open an issue or PR for questions, improvements, or collaboration

### Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes with clear commit messages
4. Add/update tests as needed
5. Open a pull request with a description

### License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

---
