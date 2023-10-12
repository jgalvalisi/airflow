from datetime import timedelta # To calculate duration
import airflow # Required to run DAG
import pandas as pd
import os

from airflow import DAG
from airflow.operators.python import PythonOperator # Will be used in our tasks to run any Python code

# get DAG directory path
dag_patch = os.getcwd()


def data_cleaning():
    dag_folder = os.path.dirname(__file__)
    data_path = os.path.join(dag_folder, 'raw_data/hotel_booking_portugal_data.csv')
    hotel_data = pd.read_csv(data_path)
    hotel_data.head() # First row
    hotel_data.info() # Printing info of our dataframe
    hotel_data.describe() # Description of our dataframe

    hotel_data.isnull().sum() # Checking null columns

    nan_replacements = {'children': 0,
                        'country': 'Unknown',
                        'agent': 'Organic Booking',
                        'company': 'Personal Booking'
                        }
    
    cleaned_data = hotel_data.fillna(nan_replacements)

    # Convert the datatypes to string

    cleaned_data['ArrivingYear'] = cleaned_data['ArrivingYear'].astype('str')
    cleaned_data['ArrivingMonth'] = cleaned_data['ArrivingMonth'].astype('str')
    cleaned_data['ArrivingDate'] = cleaned_data['ArrivingDate'].astype('str')

    cleaned_data['Canceled'] = cleaned_data['Canceled'].astype('str')
    cleaned_data['RepeatGuest'] = cleaned_data['RepeatGuest'].astype('str')

    # Combine children and babies together as kids

    cleaned_data['Kids'] = cleaned_data.Children + cleaned_data.Babies 

    cleaned_data.info()

    return cleaned_data  # Return the cleaned dataframe


def cleaned_data_message():
    print("Data successfully cleaned")


def load_data(cleaned_data): # Accept cleaned_data as a parameter
    dag_folder = os.path.dirname(__file__)
    processed_data_path = os.path.join(dag_folder, 'processed_data/hotel_booking_portugal_data.csv')
    cleaned_data.to_csv(processed_data_path, index=False)

# Initializing the default arguments we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7) # Date when the DAG should start running
}

data_cleaning_dag = DAG(
    'data_cleaning_dag',
    default_args = default_args,
    #catchup_by_default = False,
    schedule_interval = timedelta (days=1)
)

# Airflow Tasks
t1_clean_data = PythonOperator (
    task_id = 'data_cleaning',
    python_callable = data_cleaning,
    dag = data_cleaning_dag
)

t2_message = PythonOperator(
    task_id = 'cleaned_data_message',
    python_callable = cleaned_data_message,
    dag = data_cleaning_dag
)

t3_load_data = PythonOperator(
    task_id = 'load_data',
    python_callable = load_data,
    op_args = [t1_clean_data.output],  # Pass the output of t1_clean_data to t3_load_data
    dag = data_cleaning_dag
)

t1_clean_data >> t2_message >> t3_load_data