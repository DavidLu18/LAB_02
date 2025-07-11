import json
import os
import hashlib
from kafka import KafkaProducer
import time
from typing import List, Dict, Any, Set
from dotenv import load_dotenv

load_dotenv()


def calculate_message_checksum(message: Dict[str, Any], algorithm: str = "sha256") -> str:
    try:
        message_copy = {k: v for k, v in message.items() if k != 'checksum'}
        message_str = json.dumps(message_copy, sort_keys=True, default=str)
        
        if algorithm.lower() == "md5":
            hash_obj = hashlib.md5()
        elif algorithm.lower() == "sha1":
            hash_obj = hashlib.sha1()
        else:
            hash_obj = hashlib.sha256()
        
        hash_obj.update(message_str.encode('utf-8'))
        return hash_obj.hexdigest()
        
    except Exception as e:
        print(f"Error calculating checksum: {e}")
        return ""


def load_messages(file_path: str) -> List[Dict[str, Any]]:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            messages = json.load(f)
        print(f"Loaded {len(messages)} messages from {file_path}")
        return messages
    except Exception as e:
        print(f"Error loading messages: {e}")
        return []


def send_to_kafka(message: Dict[str, Any], producer: KafkaProducer, topic: str, checksum: str) -> str:
    try:
        key = message.get('key', {}).get('payload', '')
        value = message.get('value', {}).get('payload', {})
        
        value['checksum'] = checksum
        
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
    checksum_algorithm = os.getenv('CHECKSUM_ALGORITHM', 'sha256').lower()
    
    print(f"Starting Pipeline 1: {messages_file} -> Kafka topic '{topic}'")
    print(f"Checksum Algorithm: {checksum_algorithm}")
    
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
    duplicates = 0
    seen_checksums: Set[str] = set()
    
    try:
        for index, message in enumerate(messages):
            try:
                symbol = message.get('value', {}).get('payload', {}).get('symbol', 'UNKNOWN')
                
                checksum = calculate_message_checksum(message, algorithm=checksum_algorithm)
                
                if checksum in seen_checksums:
                    duplicates += 1
                    print(f"Duplicate detected {index + 1}/{len(messages)}: {symbol} - Checksum: {checksum[:8]}")
                    continue
                
                seen_checksums.add(checksum)
                
                print(f"Processing message {index + 1}/{len(messages)}: {symbol} - Checksum: {checksum[:8]}")
                
                result = send_to_kafka(message, producer, topic, checksum)
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
        print(f"Pipeline completed. Successful: {successful}, Failed: {failed}, Duplicates: {duplicates}")


if __name__ == "__main__":
    run_pipeline_1()
