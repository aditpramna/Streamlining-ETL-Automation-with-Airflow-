'''
=================================================
Milestone 3

Name: Aditya Pramana Putra
Batch: HCK 7

Code ini bertujuan untuk mengambil data dari database ,membersihkan data dan mengkoneksikannya ke dalam elasticsearch
=================================================
'''

from datetime import datetime
import psycopg2 as db
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def load_data():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3", conn)
    df.to_csv('/opt/airflow/data/P2M3_Aditya_Pramana_data_raw.csv',index=False)

def clean_data():
    '''
   Fungsi ini digunakan untuk melakukan preprocessing data dalam dataframe.

  Parameters:
   df: pandas.DataFrame - dataframe yang akan diolah

  Return
   df: pandas.DataFrame - dataframe yang telah di-preprocesss
     
    Contoh Penggunaan:

    # 1. Membaca data dari file CSV
    df = pd.read_csv('data_employee.csv')

    # 2. Memanggil fungsi preprocess_data untuk preprocessing data
    df = preprocess_data(df)
  '''
    
    df = pd.read_csv('/opt/airflow/data/P2M3_Aditya_Pramana_data_raw.csv')
    df_cleaning = df.dropna()  # Menghapus missing value pada baris data
    df_cleaning = df_cleaning.drop_duplicates()  # Menghapus duplikat data
    df_cleaning.columns = (df_cleaning.columns.str.lower()
                           .str.replace(' ', '_', regex=True))

    df.to_csv('/opt/airflow/data/P2M3_Aditya_Pramana_data_clean.csv',index=False)
    return df.values.tolist()

def push_es ():
    es = Elasticsearch("http://elasticsearch:9200") # define elasticsearch ke variable
    df_cleaned=pd.read_csv('/opt/airflow/data/P2M3_Aditya_Pramana_data_clean.csv') # import csv clean
    for i,r in df_cleaned.iterrows(): # looping untuk masuk ke elastic search
        doc=r.to_json()
        res=es.index(index="data_clean", body=doc)
        print(res)  

default_args= {
    'owner': 'Adgan',
    'start_date': datetime(2023, 10, 1) }

with DAG(
    "Milestone3_pipeline",
    description='Milestone3_pipeline',
    schedule_interval='@daily',
    default_args=default_args, 
    catchup=False) as dag:

    # Task 1
    loading_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data

    )

    # Task 2
    cleaning_data = PythonOperator(
        task_id='cleaning_data',
        python_callable=clean_data

    )

    #Task 3
    pushing_es = PythonOperator(
        task_id='push_es',
        python_callable=push_es

    )

    loading_data >> cleaning_data >> pushing_es