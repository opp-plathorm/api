from kafka import KafkaProducer,KafkaConsumer
import json

consumer = KafkaConsumer(
        'login-topic',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='1'
    )

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  
    api_version = (0,10,2),
    value_serializer=lambda v: json.dumps(v).encode('utf-8') 
    )