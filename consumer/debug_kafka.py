from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
import os
from dotenv import load_dotenv
import json

load_dotenv()

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP")
print(f"Bootstrap servers: {bootstrap_servers}")
print("=" * 60)

# Test 1: Check if we can connect to Kafka
print("\n1. Testing Kafka Connection...")
try:
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, request_timeout_ms=5000)
    cluster_metadata = admin_client.describe_cluster()
    print(f"✅ Connected! Cluster brokers: {cluster_metadata}")
    admin_client.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

# Test 2: List topics
print("\n2. Checking Topics...")
try:
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, request_timeout_ms=5000)
    topics = admin_client.list_topics()
    print(f"✅ Topics available: {list(topics.keys())}")
    admin_client.close()
except Exception as e:
    print(f"❌ Failed to list topics: {e}")

# Test 3: Check offsets for customers topic
print("\n3. Checking Topic Offsets (banking_server.public.customers)...")
try:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id='debug-group-999',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        request_timeout_ms=10000,
        session_timeout_ms=30000
    )
    
    topic = 'banking_server.public.customers'
    partitions = consumer.partitions_for_topic(topic)
    print(f"✅ Partitions for {topic}: {partitions}")
    
    if partitions:
        for partition in partitions:
            tp = (topic, partition)
            consumer.assign([tp])
            
            # Get beginning and end offsets
            consumer.seek_to_beginning(tp)
            beginning_offset = consumer.position(tp)
            
            consumer.seek_to_end(tp)
            end_offset = consumer.position(tp)
            
            print(f"   Partition {partition}: Beginning={beginning_offset}, End={end_offset}")
    
    consumer.close()
except Exception as e:
    print(f"❌ Failed to check offsets: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Actually try to read a message
print("\n4. Attempting to Read a Message...")
try:
    consumer = KafkaConsumer(
        'banking_server.public.customers',
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("   Consumer created, polling for messages...")
    msg_count = 0
    for message in consumer:
        msg_count += 1
        print(f"✅ Message {msg_count} received!")
        print(f"   Partition: {message.partition}, Offset: {message.offset}")
        if msg_count >= 1:
            break
    
    if msg_count == 0:
        print("❌ No messages received (timeout)")
    
    consumer.close()
except Exception as e:
    print(f"❌ Failed to read message: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("Diagnostic complete!")
