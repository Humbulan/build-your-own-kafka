#!/usr/bin/env python3
import socket
import struct
import time
import json

def test_persistence():
    print("=== Testing Persistence ===")
    try:
        with open("message_store.json", "r") as f:
            store = json.load(f)
            message_count = len(store.get('test-topic', {}).get('0', []))
            print(f"✓ Found {message_count} persisted messages")
    except FileNotFoundError:
        print("✗ No persistence file found yet")
    except Exception as e:
        print(f"✗ Error reading persistence file: {e}")

def test_basic_apis(host='localhost', port=9092):
    print("\n=== Testing Basic APIs ===")
    
    # Test ApiVersions
    try:
        with socket.create_connection((host, port)) as sock:
            # ApiVersions request
            api_key = 18
            api_version = 4
            correlation_id = 1001
            header = struct.pack('>hhi', api_key, api_version, correlation_id)
            request_data = header + b'\x00'  # empty client_id
            message_length = struct.pack('>i', len(request_data))
            full_request = message_length + request_data
            
            print("Sending ApiVersions request...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✓ ApiVersions Response - Correlation ID: {corr_id}")
            
    except Exception as e:
        print(f"✗ ApiVersions test failed: {e}")
        return
    
    # Test Metadata
    try:
        with socket.create_connection((host, port)) as sock:
            # Metadata request
            api_key = 3
            api_version = 9
            correlation_id = 1002
            header = struct.pack('>hhi', api_key, api_version, correlation_id)
            client_id = b'\x00\x00'  # empty string
            topics_array = b'\x00\x00\x00\x00'  # empty array
            request_data = header + client_id + topics_array
            message_length = struct.pack('>i', len(request_data))
            full_request = message_length + request_data
            
            print("Sending Metadata request...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✓ Metadata Response - Correlation ID: {corr_id}")
            
    except Exception as e:
        print(f"✗ Metadata test failed: {e}")

def test_produce_and_fetch(host='localhost', port=9092):
    print("\n=== Testing Produce + Fetch ===")
    
    # Test Produce
    try:
        with socket.create_connection((host, port)) as sock:
            # Simple Produce request
            api_key = 0
            api_version = 9
            correlation_id = 1003
            header = struct.pack('>hhi', api_key, api_version, correlation_id)
            transactional_id = b'\x00\x00'  # NULL string
            acks = struct.pack('>h', 1)
            timeout = struct.pack('>i', 30000)
            topic_array_length = b'\x00\x00\x00\x01'  # 1 topic
            
            topic_name = b'\x00\x0atest-topic'  # "test-topic"
            partition_array_length = b'\x00\x00\x00\x01'  # 1 partition
            partition_id = struct.pack('>i', 0)
            records_length = struct.pack('>i', 19)  # length of "Simple test message"
            records_data = b'Simple test message'
            
            partition_data = partition_id + records_length + records_data
            topic_data = topic_name + partition_array_length + partition_data
            
            request_data = (header + transactional_id + acks + timeout + 
                          topic_array_length + topic_data)
            message_length = struct.pack('>i', len(request_data))
            full_request = message_length + request_data
            
            print("Sending Produce request...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✓ Produce Response - Correlation ID: {corr_id}")
            
    except Exception as e:
        print(f"✗ Produce test failed: {e}")
        return
    
    # Wait a moment
    time.sleep(1)
    
    # Test Fetch
    try:
        with socket.create_connection((host, port)) as sock:
            # Simple Fetch request
            api_key = 1
            api_version = 12
            correlation_id = 1004
            header = struct.pack('>hhi', api_key, api_version, correlation_id)
            replica_id = struct.pack('>i', -1)
            max_wait_ms = struct.pack('>i', 100)
            min_bytes = struct.pack('>i', 1)
            max_bytes = struct.pack('>i', 52428800)
            isolation_level = struct.pack('>b', 0)
            topic_array_length = b'\x00\x00\x00\x01'  # 1 topic
            
            topic_name = b'\x00\x0atest-topic'  # "test-topic"
            partition_array_length = b'\x00\x00\x00\x01'  # 1 partition
            partition_id = struct.pack('>i', 0)
            fetch_offset = struct.pack('>q', 0)
            partition_max_bytes = struct.pack('>i', 1048576)
            
            partition_data = partition_id + fetch_offset + partition_max_bytes
            topic_data = topic_name + partition_array_length + partition_data
            
            request_data = (header + replica_id + max_wait_ms + min_bytes + max_bytes + 
                          isolation_level + topic_array_length + topic_data)
            message_length = struct.pack('>i', len(request_data))
            full_request = message_length + request_data
            
            print("Sending Fetch request...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✓ Fetch Response - Correlation ID: {corr_id}")
            
            # Check if we got some data
            if len(response) > 50:
                print("✓ Fetch contains message data")
            else:
                print("✗ Fetch response seems empty")
                
    except Exception as e:
        print(f"✗ Fetch test failed: {e}")

if __name__ == "__main__":
    print("Testing Enhanced Kafka Server Features")
    print("Make sure your Kafka server is running on localhost:9092\n")
    
    test_persistence()
    time.sleep(1)
    test_basic_apis()
    time.sleep(1)
    test_produce_and_fetch()
    
    print("\n=== Enhanced features test completed ===")
