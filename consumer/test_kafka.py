from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv
from threading import Thread

load_dotenv()

topics = [
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions'
]

def check_topic(topic_name):
    print(f"\n{'='*60}")
    print(f"Checking topic: {topic_name}")
    print(f"{'='*60}")
    
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
            auto_offset_reset='earliest',
            consumer_timeout_ms=10000,  # Increased timeout to 10 seconds
            session_timeout_ms=30000,  # Increase session timeout
            request_timeout_ms=40000,  # Increase request timeout
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id=f"test-group-{topic_name}"
        )

        message_count = 0
        print(f"Waiting for messages (10 second timeout)...")
        
        for message in consumer:
            print(f"\nMessage {message_count + 1}:")
            print(f"  Offset: {message.offset}")
            print(f"  Value: {json.dumps(message.value, indent=2)}")
            message_count += 1
            if message_count >= 3:  # Show max 3 messages per topic
                break

        print(f"\nTotal messages received: {message_count}")
        
    except Exception as e:
        print(f"Error checking {topic_name}: {e}")
    finally:
        if consumer:
            consumer.close()

# Check all topics
print("Testing Kafka Consumer for all topics...")
print(f"Bootstrap servers: {os.getenv('KAFKA_BOOTSTRAP')}")

for topic in topics:
    check_topic(topic)

print(f"\n{'='*60}")
print("All topics checked!")
print(f"{'='*60}")
