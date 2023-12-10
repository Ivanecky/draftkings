# Airflow DAG to run DK odds upload

# Import libraries
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import timedelta
import os
import sys

# Set system path
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

# Define the default args
default_args = {
    'owner': 'sgi',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 1)
}

# Create the DAG
dag = DAG(
    'dk_nfl_odds',
    default_args=default_args,
    description='DK NFL Odds DAG',
    schedule_interval = '* * * * *',
    start_date=airflow.utils.dates.days_ago(1),
)

# Python operator to run dk_nfl_odds.py
run_odds_scraper = BashOperator(
    task_id = 'nfl_odds_scrape',
    bash_command = 'python3.9 /Users/samivanecky/airflow/scripts/dk_nfl_odds.py',
    dag = dag
)

