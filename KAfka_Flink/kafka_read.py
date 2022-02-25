from kafka import KafkaConsumer  
from json import loads
import json
import sys
def connection():
    folderName = "/home/deq/Desktop/aiven_detail"
    consumer = KafkaConsumer(
        bootstrap_servers="kafka-prod-bepureme-d95a.aivencloud.com:12414",
        security_protocol="SSL",
        ssl_cafile=folderName+"/ca.pem",
        ssl_certfile=folderName+"/service.cert",
        ssl_keyfile=folderName+"/service.key",
        #value_deserializer=lambda v: json.dumps(v).encode('ascii'),
        #key_serializer=lambda v: json.dumps(v).encode('ascii')
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda y: json.loads(y.decode('utf-8'))     
    )
    consumer.topics()
    consumer.subscribe(topics=["Address"])
    consumer.subscription()
    print(consumer)
    for msg in consumer:
        print(msg.value)
connection()