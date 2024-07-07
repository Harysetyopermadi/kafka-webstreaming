from dash import Dash, html, dcc, callback_context, Output, Input
import plotly.express as px
from kafka import KafkaConsumer
import json
import pandas as pd

app = Dash(__name__)

# Definisikan layout Dash
app.layout = html.Div(children=[
    html.H1(children='Halo Dash'),
    html.Div(children='''
        Dash: Kerangka aplikasi web untuk data Anda.
    '''),
    dcc.Graph(
        id='contoh-grafik'
    )
    ,
    dcc.Interval(
        id='interval-component',
        interval=10000,  # Interval dalam milidetik (misalnya, 60.000 milidetik = 1 menit)
        n_intervals=0
     )
])

# Konsumen Kafka
consumer = KafkaConsumer('topik2-stream', bootstrap_servers=['192.168.0.5:9092'],
                         #auto_offset_reset='earliest',
                         api_version=(0, 10))


# Fungsi untuk memperbarui grafik
def update_graph(interval):
    # Periksa apakah ada pesan Kafka yang tersedia
    for message in consumer:
            message_value = json.loads(message.value.decode('utf-8'))
            df = pd.read_json(message_value)
            df['waktu_beli'] = pd.to_datetime(df['waktu_beli'],unit='ms')
            fig = px.bar(df, x="nm_brng", y="trx")
            fig2= px.funnel(df, x="nm_brng", y="trx")
            return fig2
    

# Callback untuk memperbarui grafik setiap interval
@app.callback(
    Output('contoh-grafik', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graph_callback(n):
    return update_graph(n)


if __name__ == '__main__':
    app.run(host='192.168.0.5', port=3000,debug=True)