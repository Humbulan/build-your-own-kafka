#!/usr/bin/env python3
import socket
import struct
import time
import json
import os

def check_persistence():
    print("=== Checking Persistence ===")
    if os.path.exists("message_store.json"):
        with open("message_store.json", "r") as f:
            store = json.load(f)
            total_messages = sum(len(messages) for topic in store.values() for messages in topic.values())
            print(f"✓ Persistence file exists: message_store.json")
            print(f"✓ Total messages in store: {total_messages}")
            for topic, partitions in store.items():
                for p_id, messages in partitions.items():
                    print(f"  - {topic}[{p_id}]: {len(messages)} messages")
                    for i, msg in enumerate(messages[-3:]):  # Show last 3 messages
                        print(f"    [{i}] {msg[:50]}...")
    else:
        print("✗ No persistence file found")

def produce_message(message_text="Test message"):
    print(f"\n=== Producing: {message_text} ===")
    try:
        with socket.create_connection(('localhost', 9092)) as sock:
            api_key = 0
            api_version = 9
            correlation_id = int(time.time() * 1000) % 10000
            header = struct.pack('>hhi', api_key, api_version, correlation_id)
            transactional_id = b'\x00\x00'
            acks = struct.pack('>h', 1)
            timeout = struct.pack('>i', 30000)
            topic_array_length = b'\x00\x00\x00\x01'
            
            topic_name = b'\x00\x0atest-topic'
            partition_array_length = b'\x00\x00\x00\x01'
            partition_id = struct.pack('>i', 0)
            records_length = struct.pack('>i', len(message_text))
            records_data = message_text.encode('utf-8')
            
            partition_data = partition_id + records_length + records_data
            topic_data = topic_name + partition_array_length + partition_data
            
            request_data = (header + transactional_id + acks + timeout + 
                          topic_array_length + topic_data)
            message_length = struct.pack('>i', len(request_data))
            full_request = message_length + request_data
            
            sock.sendall(full_request)
            
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✓ Message produced (Correlation ID: {corr_id})")
            
    except Exception as e:
        print(f"✗ Produce failed: {e}")

def test_persistence_cycle():
    print("🚀 Testing Persistence Cycle")
    print("=" * 50)
    
    # Check initial state
    check_persistence()
    
    # Produce some messages
    for i in range(3):
        produce_message(f"Persistent message {i} at {time.time()}")
        time.sleep(0.5)
    
    # Check state after produces
    print("\n" + "=" * 50)
    print("After producing messages:")
    check_persistence()
    
    print("\n" + "=" * 50)
    print("💡 Now stop the server with Ctrl+C and restart it to test persistence!")
    print("Then run this test again to verify messages survived restart.")

if __name__ == "__main__":
    test_persistence_cycle()
