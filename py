import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import telebot
from random import randint
import time
import pandas as pd
import requests
import gspread as gs
import re
import os
from sqlalchemy import create_engine,Column,String,Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import psycopg2
import tabulate
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

 from airflow import DAG
 from airflow.operators.empty import EmptyOperator

dag = DAG('ETL_test',
          schedule_interval=timedelta(days=1),
          start_date=datetime(2021, 12, 17, 0))
def test():
    print("HELLO SASHA LESNOY")

task_transform_people = PythonOperator(
    task_id='test',
    python_callable=test,
    dag=dag,
    provide_context=True
)

task_transform_people
