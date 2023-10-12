## ETL process using Docker + Python + Airflow + Superset

In this project, I mixed different tools in order to carry out an ETL process, including a brief visualization analysis. In order of appearance, the resources that I have used are:

- Docker: Python, Airflow, and Superset are based on images on Docker
- Python: create the necessary script to run DAGs within Airflow
- Airflow: set up an ETL process
- Superset: connect with Airflow ETL results and display visualizations

### Dataset

This data set contains booking information for city and resort hotels. It includes information such as when the booking was made, length of stay, the number of adults, children, and/or babies, and the number of available parking spaces, among other things. All personally identifying information (PII) has been taken off from the dataset. We will perform exploratory data analysis with Python to get insight from the data.


### Docker

ss


### Python

sss


### Airflow

After overcoming errors regarding the path of the original CSV file, both for import and export, we correctly made the connection. Now, we have the latest transformed version in the processed_data folder in our AIRFLOW_DOCKER folder.

<img width="1650" alt="Screenshot 2023-08-17 at 09 59 47" src="https://github.com/jgalvalisi/airflow/assets/97465207/88389e33-150a-462a-90c5-a6acc47b4a07">



### Superset

ss