#!/usr/bin/env python3
import socket
import struct
import time
import json
import os

def test_api_versions(host='localhost', port=9092):
    """Test ApiVersions request"""
    print("🧪 Testing ApiVersions...")
    
    api_key = 18
    api_version = 4
    correlation_id = 1001
    
    # Build request
    header = struct.pack('>hhi', api_key, api_version, correlation_id)
    client_id = b'\x00'  # Empty string
    tagged_fields = b'\x00'
    
    request_data = header + client_id + tagged_fields
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            print("📤 Sending ApiVersions request...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            error_code = struct.unpack('>h', response[4:6])[0]
            
            print(f"✅ ApiVersions Response - Correlation ID: {corr_id}, Error Code: {error_code}")
            return True
            
    except Exception as e:
        print(f"❌ ApiVersions test failed: {e}")
        return False

def test_metadata(host='localhost', port=9092):
    """Test Metadata request"""
    print("\n🧪 Testing Metadata...")
    
    api_key = 3
    api_version = 9
    correlation_id = 1002
    
    # Build request
    header = struct.pack('>hhi', api_key, api_version, correlation_id)
    client_id = b'\x00'
    topics_array_length = struct.pack('>i', 0)[-1:]  # Empty array
    allow_auto_topic_creation = b'\x01'  # True
    include_topic_authorized_operations = b'\x00'  # False
    tagged_fields = b'\x00'
    
    request_data = header + client_id + topics_array_length + allow_auto_topic_creation + include_topic_authorized_operations + tagged_fields
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            print("📤 Sending Metadata request...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✅ Metadata Response - Correlation ID: {corr_id}")
            return True
            
    except Exception as e:
        print(f"❌ Metadata test failed: {e}")
        return False

def test_produce(host='localhost', port=9092):
    """Test Produce request"""
    print("\n🧪 Testing Produce...")
    
    api_key = 0
    api_version = 9
    correlation_id = 1003
    
    # Build request
    header = struct.pack('>hhi', api_key, api_version, correlation_id)
    transactional_id = b'\x00'
    acks = struct.pack('>h', 1)
    timeout = struct.pack('>i', 30000)
    topic_data_length = struct.pack('>i', 1)[-1:]  # 1 topic
    
    # Topic data
    topic_name = b'\x0a' + b'test-topic'  # "test-topic"
    partition_data_length = struct.pack('>i', 1)[-1:]  # 1 partition
    
    # Partition data
    partition_index = struct.pack('>i', 0)
    records_length = struct.pack('>i', 45)  # Record batch length
    
    # Simple record batch
    base_offset = struct.pack('>q', 0)
    batch_length = struct.pack('>i', 37)
    partition_leader_epoch = struct.pack('>i', -1)
    magic = struct.pack('>b', 2)
    crc = struct.pack('>I', 0)
    attributes = struct.pack('>h', 0)
    last_offset_delta = struct.pack('>i', 0)
    first_timestamp = struct.pack('>q', int(time.time() * 1000))
    max_timestamp = first_timestamp
    producer_id = struct.pack('>q', -1)
    producer_epoch = struct.pack('>h', -1)
    base_sequence = struct.pack('>i', -1)
    records_count = struct.pack('>i', 1)
    
    # Single record
    record_length = struct.pack('>i', 14)
    record_attributes = struct.pack('>b', 0)
    timestamp_delta = struct.pack('>i', 0)
    offset_delta = struct.pack('>i', 0)
    key_length = struct.pack('>i', -1)
    value_length = struct.pack('>i', 5)
    value_data = b'hello'
    headers_length = struct.pack('>i', 0)
    
    record = record_length + record_attributes + timestamp_delta + offset_delta + key_length + value_length + value_data + headers_length
    
    record_batch = (base_offset + batch_length + partition_leader_epoch + magic + crc + 
                   attributes + last_offset_delta + first_timestamp + max_timestamp + 
                   producer_id + producer_epoch + base_sequence + records_count + record)
    
    partition_data = partition_index + records_length + record_batch
    topic_data_tagged_fields = b'\x00'
    topic_data = topic_name + partition_data_length + partition_data + topic_data_tagged_fields
    tagged_fields = b'\x00'
    
    request_data = (header + transactional_id + acks + timeout + 
                   topic_data_length + topic_data + tagged_fields)
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            print("📤 Sending Produce request...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✅ Produce Response - Correlation ID: {corr_id}")
            return True
            
    except Exception as e:
        print(f"❌ Produce test failed: {e}")
        return False

def test_fetch(host='localhost', port=9092):
    """Test Fetch request"""
    print("\n🧪 Testing Fetch...")
    
    api_key = 1
    api_version = 12
    correlation_id = 1004
    
    # Build request
    header = struct.pack('>hhi', api_key, api_version, correlation_id)
    replica_id = struct.pack('>i', -1)
    max_wait_ms = struct.pack('>i', 100)
    min_bytes = struct.pack('>i', 1)
    max_bytes = struct.pack('>i', 52428800)
    isolation_level = struct.pack('>b', 0)
    session_id = struct.pack('>i', 0)
    session_epoch = struct.pack('>i', -1)
    topic_array_length = struct.pack('>i', 1)[-1:]  # 1 topic
    
    # Topic data
    topic_name = b'\x0a' + b'test-topic'
    partition_array_length = struct.pack('>i', 1)[-1:]  # 1 partition
    
    # Partition data
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
    forgotten_topics_length = struct.pack('>i', 0)[-1:]
    rack_id = b'\x00'
    tagged_fields = b'\x00'
    
    request_data = (header + replica_id + max_wait_ms + min_bytes + max_bytes + 
                   isolation_level + session_id + session_epoch + topic_array_length + 
                   topic_data + forgotten_topics_length + rack_id + tagged_fields)
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            print("📤 Sending Fetch request...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✅ Fetch Response - Correlation ID: {corr_id}")
            
            # Check response length to see if we got data
            if len(response) > 50:
                print("✅ Fetch contains message data")
            else:
                print("⚠️ Fetch response is minimal (may be empty)")
                
            return True
            
    except Exception as e:
        print(f"❌ Fetch test failed: {e}")
        return False

def test_persistence():
    """Test persistence between server restarts"""
    print("\n🧪 Testing Persistence...")
    
    store_file = "kafka_persistent_store.json"
    
    # Check if store file exists
    if os.path.exists(store_file):
        try:
            with open(store_file, 'r') as f:
                store = json.load(f)
                topic_data = store.get('test-topic', {})
                partition_data = topic_data.get('0', [])
                message_count = len(partition_data)
                print(f"✅ Found {message_count} persisted messages in {store_file}")
                if message_count > 0:
                    print(f"📝 Sample message: {partition_data[0][:50]}...")
                return True
        except Exception as e:
            print(f"❌ Error reading persistence file: {e}")
            return False
    else:
        print(f"⚠️ Persistence file {store_file} not found yet")
        return False

def main():
    print("🚀 Starting Comprehensive Kafka Server Test")
    print("=" * 50)
    
    # Wait a moment for server to start if needed
    time.sleep(1)
    
    # Run all tests
    tests = [
        test_api_versions,
        test_metadata, 
        test_produce,
        test_fetch,
        test_persistence
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
            time.sleep(0.5)  # Small delay between tests
        except Exception as e:
            print(f"💥 Test {test.__name__} crashed: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    for i, (test, result) in enumerate(zip(tests, results)):
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{i+1}. {test.__name__}: {status}")
    
    print(f"\n🎯 Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests completed successfully!")
    else:
        print("⚠️ Some tests failed. Check server logs for details.")

if __name__ == "__main__":
    main()
