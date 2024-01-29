from airflow import DAG

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator

import datetime
import pendulum
import requests

default_args={
    "depends_on_past": True,
    "retries": 9,
    "retry_delay": datetime.timedelta(minutes=1)}
 
dag_ses_burn = DAG(dag_id = "Dag_forecast",
                   start_date=pendulum.datetime(2023, 3, 11),
                   default_args=default_args,
                   description="dag_forecast",
                   schedule_interval="30 9 * * *", # UTC timezone
                   catchup=False)


def weather_to_bd(gtp_code_1, gtp_code_2, **kwargs):
    r = requests.post("http://192.168.252.11:3225/weather_to_bd/", json = {'gtp_code': gtp_code_1})
    r.text
    r2 = requests.post("http://192.168.252.11:3225/weather_to_bd/", json = {'gtp_code': gtp_code_2})
    r2.text


def forecast_to_bd(gtp_code_1, gtp_code_2, **kwargs):
    r = requests.post("http://192.168.252.11:3225/forecast_to_bd/", json = {'gtp_code': gtp_code_1})
    r.text
    r2 = requests.post("http://192.168.252.11:3225/forecast_to_bd/", json = {'gtp_code': gtp_code_2})
    r2.text


def send_forecast_to_email(**kwargs):
    r = requests.post("http://192.168.252.11:3225/send_forecast_to_email/")
    r.text

weather_to_bd_t = PythonOperator(task_id='weather_to_bd',
                                  provide_context=True,
                                  execution_timeout=datetime.timedelta(seconds=180),
                                  python_callable=weather_to_bd,
                                  op_kwargs={"gtp_code_1":'test_1', "gtp_code_2":'test_2'},
                                  dag=dag_ses_burn)


forecast_to_bd_t = PythonOperator(task_id='forecast_to_bd',
                                   provide_context=True,
                                   execution_timeout=datetime.timedelta(seconds=180),
                                   python_callable=forecast_to_bd,
                                   op_kwargs={"gtp_code_1":'test_1', "gtp_code_2":'test_2'},
                                   dag=dag_ses_burn)


send_forecast_to_email_t = PythonOperator(task_id='send_forecast_to_email',
                                          provide_context=True,
                                          execution_timeout=datetime.timedelta(seconds=180),
                                          python_callable=send_forecast_to_email,
                                          dag=dag_ses_burn)

weather_to_bd_t>>forecast_to_bd_t>>send_forecast_to_email_t