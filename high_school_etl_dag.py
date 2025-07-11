# ==========================================
# AIRFLOW DAG: School Data ETL Pipeline
# ==========================================
# Purpose: Extract school data from JSON file, transform it, and load into PostgreSQL database

# Import required libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json

# ==========================================
# DAG CONFIGURATION
# ==========================================

# Define default arguments for the DAG
default_args = {
    'owner': 'shraddha',                # Owner of the DAG
    'depends_on_past': False,           # Don't depend on previous DAG runs
    'email_on_failure': False,          # Don't send email on task failure
    'email_on_retry': False,            # Don't send email on task retry
    'retries': 1,                       # Number of retries if task fails
    'retry_delay': timedelta(minutes=5), # Wait 5 minutes between retries
    'start_date': datetime(2024, 5, 1)  # When the DAG should start running
}

# ==========================================
# EXTRACTION FUNCTIONS
# ==========================================

def fetch_data_from_json(file_path):
    """
    Extract data from JSON file containing Indonesian school information
    
    Purpose: Read school data from a local JSON file instead of API call
    
    Args:
        file_path (str): Path to the JSON file containing school data
    
    Returns:
        dict: Complete JSON data as Python dictionary containing school information
        
    Note: This function replaces API calls to work with local data files
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

# ==========================================
# TRANSFORMATION FUNCTIONS
# ==========================================

def transform_data(**kwargs):
    """
    Transform raw school data according to business requirements
    
    Purpose: Clean, filter, and enhance school data for analytical purposes
    
    Transformations Applied:
    1. Filter for public high schools only (status='N' and bentuk='SMA')
    2. Create combined school address field
    3. Convert coordinates to numeric format
    4. Remove records with missing coordinate data
    
    Args:
        **kwargs: Airflow context containing task instance for XCom communication
        
    Returns:
        pandas.DataFrame: Cleaned and transformed school data ready for database loading
    """
    # Extract data from previous task using XCom
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_api')
    
    # Extract the school data from the nested JSON structure
    sekolah_data = data['dataSekolah']
    
    # Convert JSON data to pandas DataFrame for easier manipulation
    df = pd.DataFrame(sekolah_data)
    
    # TRANSFORMATION 1: Filter data for public high schools only
    # 'N' = Negeri (Public), 'SMA' = Sekolah Menengah Atas (High School)
    transformed_df = df[(df['status'].str.contains('N')) & (df['bentuk'] == 'SMA')]
    
    # TRANSFORMATION 2: Create enhanced address field
    # Combine school name with street address for better identification
    transformed_df['school_address'] = transformed_df['sekolah'] + " - " + transformed_df['alamat_jalan']
    
    # TRANSFORMATION 3: Convert coordinate columns to numeric data type
    # This ensures proper data types for geographical analysis
    transformed_df['lintang'] = pd.to_numeric(transformed_df['lintang'], errors='coerce')  # Latitude
    transformed_df['bujur'] = pd.to_numeric(transformed_df['bujur'], errors='coerce')      # Longitude
    
    # TRANSFORMATION 4: Data quality - remove records with missing coordinates
    # Essential for location-based analysis and mapping
    transformed_df = transformed_df.dropna(subset=['lintang', 'bujur'])
    
    return transformed_df

# ==========================================
# LOADING FUNCTIONS
# ==========================================

# Define target database schema (customize as needed)
custom_schema = 'hijir'

def load_data_to_database(**kwargs):
    """
    Load transformed data into PostgreSQL database
    
    Purpose: Store processed school data in database for further analysis and reporting
    
    Process:
    1. Retrieve transformed data from previous task
    2. Connect to PostgreSQL using Airflow connection
    3. Create table with appropriate schema if it doesn't exist
    4. Load data into target table, replacing existing data
    
    Args:
        **kwargs: Airflow context containing task instance for XCom communication
        
    Database Details:
    - Schema: hijir (customizable)
    - Table: target_table
    - Load Strategy: Replace existing data
    """
    # Get transformed data from previous task
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    
    # Establish connection to PostgreSQL database using Airflow connection
    postgres_hook = PostgresHook(postgres_conn_id='postgres')
    
    # Analyze data types of transformed data for table creation
    # Default to TEXT type for all columns to avoid data type conflicts
    data_types = {col: 'TEXT' for col, dtype in transformed_data.dtypes.items()}
    
    # Create table if it doesn't exist in the specified schema
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {custom_schema}.target_table (
        {', '.join([f'{col} {data_types[col]}' for col in transformed_data.columns])}
    );
    """
    postgres_hook.run(create_query)
    
    # Load transformed data into the database table
    # if_exists='replace': Drops and recreates table with new data
    transformed_data.to_sql(
        'target_table', 
        postgres_hook.get_sqlalchemy_engine(), 
        schema=custom_schema, 
        if_exists='replace', 
        index=False
    )

# ==========================================
# DAG DEFINITION
# ==========================================

# Create the DAG with specified configuration
with DAG(
    'api_to_database_dag_sekolah_hijir',  # Unique DAG identifier
    default_args=default_args,            # Apply default arguments
    start_date=datetime(2024, 5, 1),      # DAG start date
    schedule_interval='@daily',           # Run daily
    catchup=False                         # Don't run for past dates
) as dag:
    
    # ==========================================
    # TASK DEFINITIONS
    # ==========================================
    
    # EXTRACT TASK: Read data from JSON file
    extract_task = PythonOperator(
        task_id='fetch_data_from_api',    # Task identifier
        python_callable=fetch_data_from_json,  # Function to execute
        # Note: Add file_path parameter when calling this task
    )
    
    # TRANSFORM TASK: Clean and process the data
    transform_task = PythonOperator(
        task_id='transform_data',         # Task identifier
        python_callable=transform_data,   # Function to execute
        provide_context=True              # Enable access to Airflow context and XCom
    )
    
    # LOAD TASK: Store data in PostgreSQL database
    load_task = PythonOperator(
        task_id='load_data_to_database',  # Task identifier
        python_callable=load_data_to_database,  # Function to execute
        provide_context=True              # Enable access to Airflow context and XCom
    )
    
    # ==========================================
    # TASK DEPENDENCIES (ETL PIPELINE FLOW)
    # ==========================================
    
    # Define execution order: Extract → Transform → Load
    extract_task >> transform_task >> load_task

# ==========================================
# PIPELINE SUMMARY
# ==========================================
"""
ETL Pipeline Flow:
1. EXTRACT: Read high school data from JSON file
2. TRANSFORM: 
   - Filter for public high schools only
   - Create enhanced address fields
   - Convert coordinates to numeric format
   - Remove records with missing location data
3. LOAD: Store processed data in PostgreSQL database

Schedule: Daily execution
Target: High schools with valid coordinate data
Output: Clean dataset ready for analysis and reporting
"""
