import threading
import csv
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
#for cassandra
import subprocess
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys


# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'IES77KLFMXH77EZC',
    'sasl.password': 'CQRHgzu342wCGN33d9vOY7WRJQa+fU0i0AYOYuHS8v2iDWjm3KHmXbSaORzTmrYr',
    'group.id': 'group1'
    #'auto.offset.reset': 'earliest'
}


#cassandra config
cloud_config= {
  'secure_connect_bundle': "C:/Users/rohit_jcjttq8/Desktop/Data Engineering Bootcamp/HACKATHON/datasource stream/casandra/secure-connect-achievers-project.zip"
}
#cassandra config
with open("C:/Users/rohit_jcjttq8/Desktop/Data Engineering Bootcamp/HACKATHON/datasource stream/casandra/achievers_project-token.json") as f:
    secrets = json.load(f)
CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]
auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()


# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
   'url': 'https://psrc-6zww3.us-east-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('AXN73PUX2PB72WSI', 'CUCSqmFAKeame0AuqXQDxbKVo4xBC/XElZmXfRMqT0Q/Bzg76YtuKqBQnEVzRR6N')
})

# Fetch the latest Avro schema for the value
subject_name = 'ad_click-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id']
    #'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the ad_click topic
consumer.subscribe(['ad_click'])

ad_dimension_data = {}
user_dimension_data = {}


# Load ad dimension data
with open('campaigns.csv', newline='') as ad_csvfile:
    reader = csv.DictReader(ad_csvfile)
    for row in reader:
        ad_id = int(row['ad_id'])
        ad_dimension_data[ad_id] = {
            'campaign': row['campaign'],
            'product': row['product'],
            'target_start_date': row['target_start_date'],
            'target_end_date': row['target_end_date']
        }

# Load user dimension data
with open('user_demographics.csv', newline='') as user_csvfile:
    reader = csv.DictReader(user_csvfile)
    for row in reader:
        user_id = row['user_id']
        user_dimension_data[user_id] = {
            'age': int(row['age']),
            'gender': row['gender'],
            'interests': row['interests'].split(';')
        }


# Modify the enrichment function to accept both ad_id and user_id
def enrich_message(message, ad_dimension_data, user_dimension_data):
    ad_id = message['ad_id']  
    user_id = message['user_id'] 
    
    # Enrich the message with ad information
    ad_info = ad_dimension_data.get(ad_id, {})
    message.update(ad_info)
    
    # Enrich the message with user information
    user_info = user_dimension_data.get(user_id, {})
    message.update(user_info)
    
    return message

def send_to_cassandra(message):
    # Command to use a keyspace
    insert_stmt = session.prepare("""insert into achievers.click_event(ad_id, user_id, click_timestamp, platform, location, 
    campaign,product,target_start_date,target_end_date,age,gender,interests) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")


    try:
        # Parse the JSON message
        #message = json.loads(message_json)
        #interests_str = ', '.join(message['interests'])
        # Insert the message data into Cassandra
        

        cassandra_data = (
                message.get("ad_id"),
                message.get("user_id"),
                message.get("click_timestamp"),
                message.get("platform"),
                message.get("location"),
                message.get("campaign"),
                message.get("product"),
                message.get("target_start_date"),
                message.get("target_end_date"),
                message.get("age"),
                message.get("gender"),
                message.get("interests")
            )    
        session.execute(insert_stmt, cassandra_data)
        print("Message inserted into Cassandra:", message)
    except json.JSONDecodeError as e:
        print("Error parsing JSON:", e)
    except Exception as e:
        print("Error inserting message into Cassandra:", e)


# Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        value = msg.value()

        #Enrich the message with dimension data
        # Enrich the message with data from both dimension tables
        enriched_message = enrich_message(value, ad_dimension_data, user_dimension_data)
        send_to_cassandra(enriched_message)


        #print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))
        print('Successfully consumed record with value {}'.format(enriched_message))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()