import pandas as pd
from kafka import KafkaProducer
import json,time,random
from datetime import datetime


# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers='192.168.0.5:9092'
    ,value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Your Pandas DataFrame
nama_barang = ['beras', 'kopi', 'tepung','jagung']
nama_pembeli = ['yatno','berliana','butet','agus','joni']
data_dic = {}

while True:
    hrg_brng = random.randint(1000, 1000000)
    nm_brng = random.choice(nama_barang)
    nm_pmbl = random.choice(nama_pembeli)
    wkt_beli = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    data_dic['nm_brng'] = nm_brng
    data_dic['nm_pmbl'] = nm_pmbl
    data_dic['total_harga'] = hrg_brng
    data_dic['waktu_beli'] = wkt_beli
    print(data_dic)
    producer.send('topik1-main', value=data_dic)
    time.sleep(1)