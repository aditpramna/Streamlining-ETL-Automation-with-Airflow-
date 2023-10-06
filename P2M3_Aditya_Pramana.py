'''
=================================================
Milestone 3

Name: Aditya Pramana Putra
Batch: HCK 7

Tujuan code ini adalah untuk mengambil data dari database PostgreSQL, membersihkannya, dan kemudian mengindeks JSON ke Elasticsearch.
=================================================
'''

# Import library yang akan di pakai
import pandas as pd
from elasticsearch import Elasticsearch
import psycopg2 as db

# Koneksi ke database PostgreSQL
conn_string = "dbname='postgresc' host='localhost' user='postgres' password='adgan167'"
conn = db.connect(conn_string)

# Membaca data dari database PostgreSQL
df = pd.read_sql("SELECT * FROM table_M3", conn)

# Menyimpan data dalam file CSV
df.to_csv('P2M3_Aditya_Pramana_data_raw.csv', index=False)
print("-------Data Saved------")


def preprocess_data(df):
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
    df = pd.read_csv('P2M3_Aditya_Pramana_data_raw.csv')
    df_cleaning = df.dropna()  # Menghapus missing value pada baris data
    df_cleaning = df_cleaning.drop_duplicates()  # Menghapus duplikat data
    df_cleaning.columns = (df_cleaning.columns.str.lower()
                           .str.replace(' ', '_', regex=True))

    return df


# Transformasi DataFrame
df_final = preprocess_data(df) # ubah function menjadi dataframe df_final

# Menyimpan data yang telah diubah ke dalam file CSV
df_final.to_csv('P2M3_Aditya_Pramana_data_clean.csv', index=False) # simpan df kita menjadi csv data_clean

# Koneksi ke Elasticsearch
es = Elasticsearch("http://localhost:9200") # sambungan ke elasticsearch masing masing

df_finalis=pd.read_csv('P2M3_Aditya_Pramana_data_clean.csv')
# Indeksasi data ke Elasticsearch
for i, r in df_finalis.iterrows():
    doc = r.to_json()
    res = es.index(index="data_finalis", body=doc)
    print(res)