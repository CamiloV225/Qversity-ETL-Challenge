# 🖥️ Qversity Project

This repository contains a local data platform architecture for telecom analytics built with Docker Compose, PostgreSQL, Airflow, dbt, and pgAdmin. The project follows the **Bronze → Silver → Gold** (Medallion Architecture) modeling approach and addresses multiple business questions based on customer, location, calendar and payment data.

 ## Participant
 - **Name**: Juan Camilo Vargas Velez
 - **Email**: camilovargas1202@hotmail.com

---

## 🚧 Architecture

This project implements a multi-layered data pipeline:

- **Bronze Layer**: Raw data ingestion and staging from S3
- **Silver Layer**: Data cleaning, normalization, and flattening
- **Gold Layer**: Business-ready metrics and KPIs

---

## Project Structure

```
/
├── dags/                 # Airflow DAG definitions
├── dbt/                  # dbt project
│   ├── models/           # dbt models (bronze, silver, gold)
│   │   ├── bronze/       # Raw data staging
│   │   ├── silver/       # Cleaned data
│   │   └── gold/         # Business analytics
│   ├── tests/            # dbt tests
│   ├── dbt_project.yml   # dbt configuration
│   └── profiles.yml      # Database connections
├── scripts/              # Setup and utility scripts
├── data/                 # Data files
│   ├── raw/              # Raw input data
│   └── processed/        # Processed output data
├── logs/                 # Application logs
├── pgadmin/              # PgAdmin config and servers.json
├── env.example           # Environment variables template
├── .gitignore            # Python/SQL gitignore
├── .env                  # Environment variables (user-created)
├── docker compose.yml    # Docker environment setup
├── requirements.txt      # Python dependencies
└── README.md             # This file
```

---

## 🚀 Quick Start

### ✅ Prerequisites

- Docker & Docker Compose installed
- At least 4GB RAM available

---

## ⚙️ Setup Instructions

### 1. Create your own `.env` file

You must create a `.env` file at the root of the project (not included in the repo). Use the following schema:

```dotenv
COMPOSE_PROJECT_NAME=qversity

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.
_AIRFLOW_WWW_USER_USERNAME=YOUR_USERNAME
_AIRFLOW_WWW_USER_PASSWORD=YOUR_PASSWORD

# PostgreSQL Configuration
POSTGRES_USER=YOUR_USERNAME
POSTGRES_PASSWORD=YOUR_PASSWORD
POSTGRES_DB=YOUR_DB_NAME

# dbt Configuration
DBT_PROFILES_DIR=/dbt
DBT_PROJECT_DIR=/dbt

# pgadmin Configuration
PGADMIN_DEFAULT_EMAIL=your@email.com
PGADMIN_DEFAULT_PASSWORD=your_password
```

### 2. Configure dbt’s profiles.yml
Edit the file located at dbt/profiles.yml to match the .env credentials:
```
qversity:
  target: dev
  outputs:
    dev:
      type: postgres
      host: postgres
      user: YOUR_DB_USER
      password: YOUR_DB_PASSWORD
      dbname: YOUR_DB_NAME
      schema: public
      threads: 1
      port: 5432
```

### 3. Configure servers.json for pgAdmin
Edit the file located at pgadmin/servers.json so pgAdmin connects to your Postgres service:
```
{
  "Servers": {
    "Qversity Postgres": {
      "Name": "Qversity Postgres",
      "Group": "Servers",
      "Host": "postgres",
      "Port": 5432,
      "MaintenanceDB": "YOUR_DB_NAME",
      "Username": "YOUR_DB_USER",
      "SSLMode": "prefer"
    }
  }
}
```

## 🌐 Access Points

- **Airflow UI**: http://localhost:8080 
- **PostgreSQL**: localhost:5432
- **PgAdmin**: http://localhost:80 (PgAdmin might ask you to setup a password to enter the interface for the first time & the password for the database)

## Basic steps to Run the Project

First, execute the docker compose file to iniciate all the services needed for the project
```bash
- docker compose up -d
```

Once everything is running, simply open your browser and navigate to the Airflow's access point:
```bash
- http://localhost:8080 #(You need to enter the creditials for Airflow)
```
and then search for the Dag called qversity_project_JCV and run it.

Or execute the following command on the terminal:
```bash
- docker compose exec airflow airflow dags trigger qversity_project_JCV
```
Once the DAG has successfully completed, open the pgAdmin access point in your browser. Log in using your credentials and navigate to the “Qversity” database. Under the “Schemas” tab, you’ll find the schemas corresponding to each layer of the data pipeline.

<p align="center">
  <img width="200" alt="Database_structure" src="https://github.com/user-attachments/assets/3de7c1b6-6ce7-41c2-979b-66730111b3c3" />
</p>

Inside them, you’ll see all the resulting tables, including those that answer the business questions defined in the Gold layer. To see them you can right-click them, you will see the tab "View/Edit Data" and select "All Rows".
<p align="center">
  <img width="236" alt="schema" src="https://github.com/user-attachments/assets/6226aee7-5271-4ce4-be6a-e248b0d6e580" />
</p>

If you need to check how is everything built, check the image below to see the relation between tables on the silver layer.

<p align="center">
  <img width="650" height="500" alt="schema" src="https://github.com/user-attachments/assets/59a49d13-ec6a-4088-8d2f-eda1a824bac2" />
</p>


## Useful Commands

### Airflow
```bash
# View Airflow logs
docker compose logs -f airflow

# Trigger a DAG manually
docker compose exec airflow airflow dags trigger qversity_project_JCV
```

### DBT
```bash
# To enter the dbt container use:
docker compose exec dbt bash

# Run all models
dbt run

# Run specific layer
dbt run --select bronze
dbt run --select silver
dbt run --select gold

# Test data quality
dbt test
```

## Docker
```bash
# Executes the docker-compose file
- docker compose up -d

# Check service status
- docker compose ps

# View all service logs
- docker compose logs -f

# View specific service logs
- docker compose logs -f airflow
- docker compose logs -f dbt
- docker compose logs -f postgres

# Stop services
- docker compose down

# Remove volumes (⚠️ deletes all data)
- docker compose down -v
```

## Testing

```bash
# Run dbt tests
docker compose exec dbt dbt test

# Run specific test
docker compose exec qversity-dbt-1 dbt run --select silver
```

