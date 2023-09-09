import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import random
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd

ad_ids_producer1 = list(range(1001, 1021))
user_ids = [
    "U9239", "U4534", "U4068", "U6347", "U8632",
    "U9706", "U9237", "U1653", "U4897", "U4009",
    "U4797", "U4439", "U0372", "U1429", "U7586",
    "U9797", "U7221", "U9615", "U9764", "U4991",
    "U4824", "U4075", "U1227", "U5411", "U8129",
    "U8420", "U8500", "U7619", "U8732", "U0374",
    "U7802", "U3632", "U7504", "U6266", "U4731",
    "U9816", "U1188", "U8948", "U3009", "U0422",
    "U9133", "U5745", "U5674", "U8983", "U3567",
    "U6297", "U3380", "U7796", "U3551", "U5373",
    "U9118", "U1660", "U0535", "U6634", "U3563",
    "U8939", "U8171", "U8441", "U1089", "U4453",
    "U9248", "U0162", "U6300", "U9765", "U7533",
    "U4898", "U0778", "U0719", "U7972", "U7969",
    "U0395", "U9298", "U6746", "U2377", "U3959",
    "U1916", "U0235", "U5170", "U5476", "U9770",
    "U9431", "U9279", "U0421", "U6429", "U8601",
    "U8911", "U2362", "U1667", "U4594", "U3541",
    "U1209", "U6496", "U4909", "U6341", "U2689",
    "U8661", "U0550", "U2709", "U2241", "U8835"
]

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'IES77KLFMXH77EZC',
    'sasl.password': 'CQRHgzu342wCGN33d9vOY7WRJQa+fU0i0AYOYuHS8v2iDWjm3KHmXbSaORzTmrYr'
}

#Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-6zww3.us-east-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('AXN73PUX2PB72WSI', 'CUCSqmFAKeame0AuqXQDxbKVo4xBC/XElZmXfRMqT0Q/Bzg76YtuKqBQnEVzRR6N')
})

# Fetch the latest Avro schema for the value
subject_name = 'ad_click-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer_main = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})

# Function to generate a random click event for Producer 1
def generate_click_event_producer1():
    ad_id = random.choice(ad_ids_producer1)
    user_id = random.choice(user_ids)
    click_timestamp = (datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 2), hours=random.randint(1, 24))).strftime("%Y-%m-%d %H:%M:%S")
    platform = random.choice(["Web", "Mobile", "Tablet", "Laptop"])
    location = random.choice(["New York", "Los Angeles", "San Francisco", "India", "Russia", "Bangkok"])

    return {
        "ad_id": ad_id,
        "user_id": user_id,
        "click_timestamp": click_timestamp,
        "platform": platform,
        "location": location
    }

while True:
    # Generate and publish data for Stream 2
    data = generate_click_event_producer1()
    print(data)
    # Assuming the topic name for Stream 2 is 'stream2_topic'
    producer_main.produce(topic='ad_click', value=data)
    producer_main.flush()
    time.sleep(1)


