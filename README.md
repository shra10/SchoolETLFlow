# SchoolETLFlow

A simple ETL pipeline built with Apache Airflow for processing high school data from JSON files into PostgreSQL database.

## Overview

**SchoolETLFlow** extracts high school data from JSON files, transforms it, and loads it into PostgreSQL for analysis.

**Created by:** Shraddha Gund

## Repository Structure

```
SchoolETLFlow/
├── README.md
├── school_data.json              # High school data file
└── high_school_etl_dag.py  # Airflow DAG file
```

## What It Does

1. **Extract**: Reads high school data from JSON file
2. **Transform**: 
   - Filters for public high schools only
   - Creates combined school address field
   - Converts coordinates to numeric format
   - Removes records with missing location data
3. **Load**: Stores cleaned data in PostgreSQL database

## Prerequisites

- Apache Airflow 2.0+
- PostgreSQL
- Python packages: `pandas`, `airflow-providers-postgres`

## Setup

### 1. Install Dependencies
```bash
pip install pandas apache-airflow apache-airflow-providers-postgres
```

### 2. Configure Database Connection
In Airflow UI, create PostgreSQL connection:
- **Connection ID**: `postgres`
- **Host**: Your PostgreSQL host
- **Database**: Your database name
- **Username/Password**: Your credentials

### 3. Deploy DAG
Copy `high_school_etl_dag.py` to your Airflow DAGs folder:
```bash
cp high_school_etl_dag.py $AIRFLOW_HOME/dags/
```

## Usage

1. Place `school_data.json` in accessible location
2. Update file path in the DAG if needed
3. Enable DAG in Airflow UI
4. Pipeline runs daily automatically

## Configuration

### Key Settings (in DAG file)
- **Schedule**: Daily (`@daily`)
- **Database Schema**: `shraddha`
- **Target Table**: `target_table`
- **Retries**: 1 attempt with 5-minute delay

### Customize Schema
```python
custom_schema = 'your_schema_name'  # Line 108 in DAG file
```

## Data Flow

```
JSON File → Airflow ETL → PostgreSQL Database
    ↓           ↓             ↓
Raw Data → Clean & Filter → Stored Data
```

## Monitoring

- View progress in Airflow UI: `http://localhost:8080`
- Check logs for each task
- Verify data in PostgreSQL table

## Output

The pipeline creates a table with:
- School information
- Geographic coordinates
- Combined address fields
- Only public high schools with valid location data

---

**Created by Shraddha Gund**
