# Market Pulse

Market Pulse is a stock market data analytics platform built to demonstrate an end-to-end Data Engineering workflow, from data ingestion to analytical data marts.

The project uses Python to ingest stock market data, PostgreSQL to store the data, dbt to transform and model it, and Apache Airflow to orchestrate the entire pipeline. Docker and Docker Compose provide a reproducible local development environment.

## Architecture

```text
Stock Market API
       │
       ▼
Python Ingestion
       │
       ▼
PostgreSQL
  └── raw
       │
       ▼
      dbt
       │
       ├── staging
       ├── intermediate
       └── marts
       │
       ▼
Analytical Data
```

Airflow orchestrates the pipeline:

```text
Ingestion
    │
    ▼
Load Raw Data
    │
    ▼
dbt Transformations
    │
    ▼
dbt Tests
    │
    ▼
Data Marts
```

## Tech Stack

| Technology     | Purpose                          |
| -------------- | -------------------------------- |
| Python         | Data ingestion and processing    |
| Apache Airflow | Pipeline orchestration           |
| PostgreSQL     | Data storage                     |
| SQL            | Data querying and transformation |
| dbt            | Data transformation and modeling |
| Docker         | Containerization                 |
| Docker Compose | Local infrastructure             |
| Git            | Version control                  |

## Pipeline Layers

### Raw

Contains data loaded from the external stock market data source with minimal transformation.

### Staging

Cleans and standardizes the raw data using dbt.

### Intermediate

Contains reusable transformation logic used to prepare data for analytical models.

### Data Marts

Contains business-oriented datasets designed for analytics and reporting.

## Local Setup

### Prerequisites

Install:

* Git
* Docker
* Docker Compose
* Python 3.10+
* PostgreSQL client
* dbt with the PostgreSQL adapter

### 1. Clone the repository

```bash
git clone <https://github.com/devharnold/marketpulse>
cd marketpulse
```

### 2. Configure environment variables

Create a `.env` file:

```bash
cp .env.example .env
```

Update the values in `.env` with the required database and API configuration.

Never commit `.env` or API credentials to Git.

### 3. Start Airflow and PostgreSQL

Market Pulse uses Docker Compose for the local infrastructure.

```bash
docker compose up -d
```

Check the running services:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

### 4. Open Airflow

Airflow is available at:

```text
http://localhost:8080
```

Use the credentials configured by the Docker Compose setup.
This is assuming that you have mapped all the deps to the airflow's container.

### 5. Configure dbt

Install the PostgreSQL adapter:

```bash
uv add dbt-postgres
```

Configure the dbt PostgreSQL connection in:

```text
~/.dbt/profiles.yml
```

Test the connection:

```bash
dbt debug
```

### 6. Run dbt

From the dbt project directory:

```bash
dbt build
```

This executes the dbt models and their associated tests.

### 7. Run the Pipeline

Trigger the Market Pulse DAG from the Airflow web interface.

The DAG coordinates:

1. Stock market data ingestion
2. Data validation
3. Loading data into PostgreSQL
4. dbt transformations
5. dbt data quality tests
6. Creation of analytical data marts

## Useful Commands

```bash
# Start the project
docker compose up -d

# Check services
docker compose ps

# View logs
docker compose logs -f

# Stop the project
docker compose down

# Restart services
docker compose restart

# Rebuild containers
docker compose up -d --build

# Run dbt
dbt build

# Test dbt connection
dbt debug
```

## Project Objective

Market Pulse is primarily a Data Engineering project focused on demonstrating the complete lifecycle of analytical data:

**Ingest → Store → Transform → Test → Model → Analyze**
