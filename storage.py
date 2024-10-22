from pyflink.datastream.connectors.jdbc import JdbcSink
from pyflink.table import DataTypes
from pyflink.datastream import SinkFunction
from pyflink.table.descriptors import Schema, Rowtime
from influxdb_client import InfluxDBClient, Point, WriteOptions
import datetime

# Crear un conector JDBC para guardar en una base de datos
JdbcSink.sink(
    'INSERT INTO weather_table (city, temp, timestamp) VALUES (?, ?, ?)',
    types=[DataTypes.STRING(), DataTypes.FLOAT(), DataTypes.TIMESTAMP()],
    connection_options={
        'url': 'jdbc:mysql://localhost:3306/mydatabase',
        'username': 'Cacheiro',
        'password': 'Zapatillas8'
    }
)

# Configuración de InfluxDB
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "my-bucket"

# Clase para escribir en InfluxDB
class InfluxDBSink(SinkFunction):
    def __init__(self):
        self.client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        self.write_api = self.client.write_api(write_options=WriteOptions(batch_size=1, flush_interval=1_000))

    def invoke(self, value, context):
        # Aquí puedes transformar el valor a un punto de InfluxDB
        point = (
            Point("weather")
            .tag("city", value['city'])
            .field("temperature", value['temperature'])
            .field("humidity", value['humidity'])
            .time(datetime.datetime.utcnow())  # Usa la hora actual
        )
        self.write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)