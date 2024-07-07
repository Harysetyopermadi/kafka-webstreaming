from kafka import KafkaConsumer,KafkaProducer
import pandas as pd
import json,time
from dash import Dash, html, dcc
import plotly.express as px
import pyodbc
import warnings
import os
from datetime import datetime,date,timedelta
warnings.filterwarnings('ignore')

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

consumer = KafkaConsumer('topik1-main', bootstrap_servers=['192.168.0.5:9092']
                         #,auto_offset_reset='earliest'
                         , api_version=(0, 10))

producer = KafkaProducer(
    bootstrap_servers='192.168.0.5:9092'
    ,value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

waktu_now=datetime.now()
result=waktu_now + timedelta(seconds=10)

r_sql=pd.read_sql("select \
                                    nm_brng, \
                                    sum(total_harga) total_harga, \
                                    convert(date,waktu_beli) waktu_beli, \
                                    count(*) trx \
                                    from topikstream where CONVERT(date,waktu_beli)= CONVERT(date,GETDATE()) \
                                    group by \
                                    nm_brng, \
                                    convert(date,waktu_beli)",cnxn)
r_sql['waktu_beli']=pd.to_datetime(r_sql['waktu_beli']).dt.date
df_tojson1=r_sql.to_json()
producer.send('topik2-stream', value=df_tojson1)

df_merge = pd.DataFrame(r_sql)

for message in consumer:
    message_value = json.loads(message.value.decode('utf-8'))
    df = pd.DataFrame(message_value.items())
    df=df.T.reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df[1:]
    #df['waktu_beli']=pd.to_datetime(df['waktu_beli']).dt.hour
    df['trx']=1
    df['waktu_beli']=pd.to_datetime(df['waktu_beli']).dt.date
    #df=df.drop('waktu_beli',axis=1)
    df=df.drop('nm_pmbl',axis=1)

    df_merge = df_merge.append(df, ignore_index=True)
    df_merge = df_merge[df_merge['waktu_beli'] >= datetime.now().date()]
    df_merge=df_merge.groupby(['nm_brng','waktu_beli']).agg({'total_harga': 'sum', 'trx': 'sum'}).reset_index()
    os.system('cls')
    #df['waktu_beli']=pd.to_datetime(df['waktu_beli']).dt.hour
    #df=df.drop('waktu_beli',axis=1)
    waktu_now=datetime.now()
    if waktu_now>result:
        result=waktu_now + timedelta(seconds=10)
        print(waktu_now)
        print("Waktu melewati ",result)
        print(df_merge)
        df_tojson=df_merge.to_json()
        print(df_tojson)
        producer.send('topik2-stream', value=df_tojson)
        #time.sleep(10)
    else:
        print(waktu_now)
        print("Waktu Belum Melewati ",result)
        #time.sleep(1)
        print(df_merge)
    
#buat koneksi get dari sql tapi buat dulu datanya di sql dan tabelnya juga