from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import storage

def process_stream(env):

    # Configuración del consumidor de Kafka
    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    # Consumir desde el tópico de Kafka (ej: weather_topic)
    kafka_consumer = FlinkKafkaConsumer(
        topics='weather_topic',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props)

    # Crear un DataStream a partir del consumidor de Kafka
    stream = env.add_source(kafka_consumer)

    # Proceso de transformación (por ejemplo, convertir el JSON en objetos Python)
    stream = stream.map(lambda x: eval(x), output_type=Types.PYTHON_OBJECT)

    # Definir una operación de procesamiento simple (ejemplo: filtrado)
    stream = stream.filter(lambda data: data['main']['temp'] > 38)

    stream.print()
    stream.add_sink(storage.InfluxDBSink())

    env.execute("Flink Kafka Consumer Job")
