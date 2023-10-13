## ETL process using Docker + Python + Airflow + Superset

In this project, I mixed different tools in order to carry out a brief ETL process based on Apache Airflow, including a brief visualization analysis. In order of appearance, the resources that I have used are:

- Docker: Python, Airflow, and Superset are based on images on Docker
- Python: create the necessary script to run DAGs within Airflow
- Airflow: set up an ETL process creating three DAGs
- Superset: connect with the Airflow ETL results and display visualizations

### Dataset

This data set contains booking information for city and resort hotels. It includes information such as when the booking was made, length of stay, the number of adults, children, and/or babies, and the number of available parking spaces, among other things. All personally identifying information (PII) has been taken off from the dataset. We will perform exploratory data analysis with Python to get insight from the data.


### Docker

We will use Docker as the base tool to accomplish our ETL process through Docker images.

- The first thing we’ll need is the docker-compose.yaml file taken from the official GitHub repo.
-  After that, connect the other services with this docker-compose.
-  Set up Airflow in port 8080.

### Python

With Python, we will accomplish two things: 

#### I) Creating a .py file to run on Airflow

#### II) Carrying out data cleaning steps such as:

   1) Checking the missing values in the columns
   2) Replace null values
   3) Modifying datatypes
   4) Deriving new variables

### Airflow

We will perform three DAGs and tasks:

#### 1) data_cleaning

As a part of the Transformation of the ETL process, we have performed some changes to the dataset with the aim of getting the CSV file ready for the corresponding analysis.
   
#### 2) clenaed_data_message

Deploying a message establishing that the data cleaning process was successfully done.

#### 3) load_data

Load the final and cleaned CSV file ready to be used in Superset.

Regarding the ETL process, we dealt with some troubles when running our DAGs. In detail, after overcoming errors regarding the path of the original CSV file, both for import and export, we correctly made the connection. Now, we have the latest transformed version in the processed_data folder in our AIRFLOW_DOCKER folder.

<img width="1650" alt="Screenshot 2023-08-17 at 09 59 47" src="https://github.com/jgalvalisi/airflow/assets/97465207/88389e33-150a-462a-90c5-a6acc47b4a07">


### Superset

- Star a Superset instance with Docker in port 8088.
- Access with your local host and add a Database connection.
- When we've done that, we will be able to answer the following business questions:

   - What are the nationalities that booked the most? And not considering people from Portugal?
   - How are the prices evolution over the year considering the type of hotel?
   - Number of reservations per day
   - Share of reservations per hotel type (City Hotel or Resort Hotel)
   - Cancellation Distributions according to different parameters  
   - Booking changes patterns
   
