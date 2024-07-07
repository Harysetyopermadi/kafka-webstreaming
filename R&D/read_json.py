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


consumer = KafkaConsumer('topik2-stream', bootstrap_servers=['192.168.0.5:9092']
                         #,auto_offset_reset='earliest'
                         , api_version=(0, 10))



for message in consumer:
    message_value = json.loads(message.value.decode('utf-8'))
    df = pd.read_json(message_value)
    os.system('cls')
    df['waktu_beli'] = pd.to_datetime(df['waktu_beli'],unit='ms')
    print(df)
