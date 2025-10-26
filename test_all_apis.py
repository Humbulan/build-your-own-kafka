#!/usr/bin/env python3
import socket
import struct
import time

def test_api_versions(host='localhost', port=9092):
    """Test ApiVersions API"""
    print("=== Testing ApiVersions API ===")
    api_key = 18
    api_version = 4
    correlation_id = 1001
    
    header_data = struct.pack('>hhi', api_key, api_version, correlation_id)
    client_id = b'\x00'
    tagged_fields = b'\x00'
    
    request_data = header_data + client_id + tagged_fields
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port)) as sock:
            print(f"Sending ApiVersions request ({len(full_request)} bytes)...")
            sock.sendall(full_request)
            
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            resp_corr_id = struct.unpack('>i', response[0:4])[0]
            error_code = struct.unpack('>h', response[4:6])[0]
            
            print(f"✓ ApiVersions Response - Correlation ID: {resp_corr_id}, Error Code: {error_code}")
            
    except Exception as e:
        print(f"✗ ApiVersions test failed: {e}")

def test_metadata(host='localhost', port=9092):
    """Test Metadata API"""
    print("\n=== Testing Metadata API ===")
    api_key = 3
    api_version = 9
    correlation_id = 1002
    
    header_data = struct.pack('>hhi', api_key, api_version, correlation_id)
    topics_array_length = b'\x00'  # Empty topics array
    allow_auto_topic_creation = b'\x01'
    include_cluster_auth = b'\x00'
    include_topic_auth = b'\x00'
    tagged_fields = b'\x00'
    
    request_data = (header_data + topics_array_length + allow_auto_topic_creation + 
                   include_cluster_auth + include_topic_auth + tagged_fields)
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port)) as sock:
            print(f"Sending Metadata request ({len(full_request)} bytes)...")
            sock.sendall(full_request)
            
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            resp_corr_id = struct.unpack('>i', response[0:4])[0]
            throttle_time = struct.unpack('>i', response[4:8])[0]
            
            print(f"✓ Metadata Response - Correlation ID: {resp_corr_id}, Throttle: {throttle_time}ms")
            
    except Exception as e:
        print(f"✗ Metadata test failed: {e}")

def test_produce(host='localhost', port=9092):
    """Test Produce API"""
    print("\n=== Testing Produce API ===")
    api_key = 0
    api_version = 9
    correlation_id = 1003
    
    header_data = struct.pack('>hhi', api_key, api_version, correlation_id)
    transactional_id = b'\x00'  # NULL
    acks = struct.pack('>h', 1)  # Wait for leader
    timeout = struct.pack('>i', 30000)  # 30 seconds
    
    # Topic data array (1 topic)
    topic_data_length = b'\x02'  # UVarInt for 1
    
    # Topic name
    topic_name = b'\x16' + b'test-topic'
    
    # Partition data array (1 partition)
    partition_data_length = b'\x02'  # UVarInt for 1
    
    # Partition data
    partition_index = struct.pack('>i', 0)
    records_length = struct.pack('>i', 10)  # Empty records for now
    records_data = b'test_data'
    
    partition_data = partition_index + records_length + records_data
    topic_data_tagged_fields = b'\x00'
    
    topic_data = topic_name + partition_data_length + partition_data + topic_data_tagged_fields
    
    tagged_fields = b'\x00'
    
    request_data = (header_data + transactional_id + acks + timeout + 
                   topic_data_length + topic_data + tagged_fields)
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port)) as sock:
            print(f"Sending Produce request ({len(full_request)} bytes)...")
            sock.sendall(full_request)
            
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            resp_corr_id = struct.unpack('>i', response[0:4])[0]
            
            print(f"✓ Produce Response - Correlation ID: {resp_corr_id}")
            
    except Exception as e:
        print(f"✗ Produce test failed: {e}")

def test_fetch(host='localhost', port=9092):
    """Test Fetch API"""
    print("\n=== Testing Fetch API ===")
    api_key = 1
    api_version = 12
    correlation_id = 1004
    
    header_data = struct.pack('>hhi', api_key, api_version, correlation_id)
    replica_id = struct.pack('>i', -1)  # -1 for consumer
    max_wait_ms = struct.pack('>i', 100)
    min_bytes = struct.pack('>i', 1)
    max_bytes = struct.pack('>i', 52428800)
    isolation_level = struct.pack('>b', 0)
    session_id = struct.pack('>i', 0)
    session_epoch = struct.pack('>i', -1)
    
    # Topic array (1 topic)
    topic_array_length = b'\x02'  # UVarInt for 1
    
    # Topic
    topic_name = b'\x16' + b'test-topic'
    
    # Partition array (1 partition)
    partition_array_length = b'\x02'  # UVarInt for 1
    
    # Partition
    partition_index = struct.pack('>i', 0)
    current_leader_epoch = struct.pack('>i', -1)
    fetch_offset = struct.pack('>q', 0)
    last_fetched_epoch = struct.pack('>i', -1)
    log_start_offset = struct.pack('>q', 0)
    partition_max_bytes = struct.pack('>i', 1048576)
    partition_tagged_fields = b'\x00'
    
    partition_data = (partition_index + current_leader_epoch + fetch_offset + 
                     last_fetched_epoch + log_start_offset + partition_max_bytes + 
                     partition_tagged_fields)
    
    topic_tagged_fields = b'\x00'
    
    topic_data = topic_name + partition_array_length + partition_data + topic_tagged_fields
    
    forgotten_topics_length = b'\x00'  # UVarInt for 0
    rack_id = b'\x00'  # NULL compact string
    tagged_fields = b'\x00'
    
    request_data = (header_data + replica_id + max_wait_ms + min_bytes + max_bytes + 
                   isolation_level + session_id + session_epoch + topic_array_length + 
                   topic_data + forgotten_topics_length + rack_id + tagged_fields)
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port)) as sock:
            print(f"Sending Fetch request ({len(full_request)} bytes)...")
            sock.sendall(full_request)
            
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            resp_corr_id = struct.unpack('>i', response[0:4])[0]
            throttle_time = struct.unpack('>i', response[4:8])[0]
            
            print(f"✓ Fetch Response - Correlation ID: {resp_corr_id}, Throttle: {throttle_time}ms")
            
    except Exception as e:
        print(f"✗ Fetch test failed: {e}")

if __name__ == "__main__":
    print("Starting comprehensive Kafka API tests...")
    print("Make sure your Kafka server is running on localhost:9092\n")
    
    test_api_versions()
    time.sleep(1)
    test_metadata()
    time.sleep(1)
    test_produce()
    time.sleep(1)
    test_fetch()
    
    print("\n=== All tests completed ===")
