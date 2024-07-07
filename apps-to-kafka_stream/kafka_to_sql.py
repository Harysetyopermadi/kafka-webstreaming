from kafka import KafkaConsumer
import pandas as pd
import json,time
from dash import Dash, html, dcc
import plotly.express as px
import pyodbc
from datetime import datetime

#koneksi
# Define the connection parameters
try:
    server = 'localhost'
    database = 'DB_Test'
    username = 'hary'
    password = '1234'
    # Create a connection string
    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    # Establish a connection
    cnxn = pyodbc.connect(connection_string,autocommit=True)
    # Create a cursor object to interact with the database
    cursor = cnxn.cursor()
    print("Koneksi Berhasil")
except:
    print("Gagal Koneksi")
# Initialize Kafka consumer
consumer = KafkaConsumer('topik1-main', bootstrap_servers=['192.168.0.5:9092']
                         ,auto_offset_reset='earliest'
                         , api_version=(0, 10))

df = pd.DataFrame()  # Initialize an empty list to store DataFrames
waktu_now=datetime.now().strftime("%Y-%m-%d")
print(waktu_now)
# Consume messages from Kafka topic
cursor.execute("delete from topikstream where convert(date,waktu_beli)='"+waktu_now+"'")
for message in consumer:
    message_value = json.loads(message.value.decode('utf-8'))
    df=pd.DataFrame.from_dict(message_value, orient='index')
    df=df.T.reset_index(drop=True)
    df['waktu_beli']=pd.to_datetime(df['waktu_beli'])
   
    df=df[(df['waktu_beli']>=waktu_now)]
    
    print(df)
    #df = df.append(message_value, ignore_index=True)
    for index,row in df.iterrows():
        cursor.execute("insert into topikstream (nm_brng,nm_pmbl,total_harga,waktu_beli) \
                       values (?,?,?,?)", row['nm_brng'],row['nm_pmbl'], row['total_harga'], row['waktu_beli'])
    
    # df_grouped = df.groupby("nm_brng").size().reset_index(name="Total")
    # print(df_grouped,end="\r")


