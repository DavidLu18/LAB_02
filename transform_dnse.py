import json
import os
from kafka import KafkaProducer
import time
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()


def load_messages(file_path: str) -> List[Dict[str, Any]]:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            messages = json.load(f)
        print(f"Loaded {len(messages)} messages from {file_path}")
        return messages
    except Exception as e:
        print(f"Error loading messages: {e}")
        return []


def send_to_kafka(message: Dict[str, Any], producer: KafkaProducer, topic: str) -> str:
    try:
        key = message.get('key', {}).get('payload', '')
        value = message.get('value', {}).get('payload', {})
        
        future = producer.send(
            topic,
            key=key.encode('utf-8') if key else None,
            value=json.dumps(value).encode('utf-8')
        )
        
        future.get(timeout=10)
        return f"Sent message for symbol: {value.get('symbol', 'UNKNOWN')}"
        
    except Exception as e:
        return f"Error sending message to Kafka: {e}"


def run_pipeline_1():
    kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9092')
    messages_file = 'messages.json'
    topic = 'dnse.raw'
    
    print(f"Starting Pipeline 1: {messages_file} -> Kafka topic '{topic}'")
    
    messages = load_messages(messages_file)
    if not messages:
        print("No messages to process")
        return
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_servers.split(','),
            retries=3,
            acks='all'
        )
        print(f"Connected to Kafka servers: {kafka_servers}")
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return
    
    successful = 0
    failed = 0
    
    try:
        for index, message in enumerate(messages):
            try:
                symbol = message.get('value', {}).get('payload', {}).get('symbol', 'UNKNOWN')
                print(f"Processing message {index + 1}/{len(messages)}: {symbol}")
                
                result = send_to_kafka(message, producer, topic)
                if "Error" not in result:
                    successful += 1
                else:
                    failed += 1
                    print(f"Failed: {result}")
                
                time.sleep(0.001)
                
            except Exception as e:
                failed += 1
                print(f"Error processing message {index}: {e}")
                
    finally:
        producer.close()
        print(f"Pipeline completed. Successful: {successful}, Failed: {failed}")


if __name__ == "__main__":
    run_pipeline_1()
