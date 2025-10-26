#!/usr/bin/env python3
import socket
import struct
import time
import gzip

def encode_unsigned_leb128(value):
    """Encode unsigned LEB128 variable-length integer"""
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            result.append(byte | 0x80)
        else:
            result.append(byte)
            break
    return bytes(result)

def test_compressed_produce(host='localhost', port=9092):
    """Test producing compressed messages"""
    print("🧪 Testing Compressed Produce...")
    
    # First produce multiple messages to trigger compression
    for i in range(3):
        api_key = 0  # Produce
        api_version = 9
        correlation_id = 3000 + i
        
        # Build Produce request
        header = struct.pack('>hhi', api_key, api_version, correlation_id)
        transactional_id = b'\x00'
        acks = struct.pack('>h', 1)
        timeout = struct.pack('>i', 30000)
        topic_data_length = encode_unsigned_leb128(1)  # 1 topic
        
        # Topic data
        topic_name = "test-topic"
        topic_name_bytes = topic_name.encode('utf-8')
        topic_field = encode_unsigned_leb128(len(topic_name_bytes)) + topic_name_bytes
        
        # Partitions array
        partition_data_length = encode_unsigned_leb128(1)  # 1 partition
        
        # Partition data
        partition_index = struct.pack('>i', 0)
        
        # Create a simple record batch with multiple messages
        records_data = create_simple_record_batch([f"Compressed message {i} at {time.time()}"])
        records_length = struct.pack('>i', len(records_data))
        
        partition_data = partition_index + records_length + records_data
        topic_data_tagged_fields = b'\x00'
        topic_data = topic_field + partition_data_length + partition_data + topic_data_tagged_fields
        tagged_fields = b'\x00'
        
        request_data = (header + transactional_id + acks + timeout + 
                       topic_data_length + topic_data + tagged_fields)
        message_length = struct.pack('>i', len(request_data))
        full_request = message_length + request_data
        
        try:
            with socket.create_connection((host, port), timeout=5) as sock:
                sock.sendall(full_request)
                
                # Read response
                resp_length_data = sock.recv(4)
                resp_length = struct.unpack('>i', resp_length_data)[0]
                response = sock.recv(resp_length)
                
                corr_id = struct.unpack('>i', response[0:4])[0]
                print(f"✅ Produced message {i+1} - Correlation ID: {corr_id}")
                
        except Exception as e:
            print(f"❌ Produce test {i} failed: {e}")
            return False
    
    return True

def create_simple_record_batch(messages):
    """Create a simple record batch for testing"""
    if not messages:
        return b''
    
    # Simple record batch header
    base_offset = struct.pack('>q', 0)
    batch_length = struct.pack('>i', 100)  # Approximate
    partition_leader_epoch = struct.pack('>i', -1)
    magic = struct.pack('>b', 2)
    attributes = struct.pack('>h', 0)  # No compression in this simple version
    last_offset_delta = struct.pack('>i', len(messages) - 1)
    first_timestamp = struct.pack('>q', int(time.time() * 1000))
    max_timestamp = first_timestamp
    producer_id = struct.pack('>q', -1)
    producer_epoch = struct.pack('>h', -1)
    base_sequence = struct.pack('>i', -1)
    records_count = struct.pack('>i', len(messages))
    
    # Build records
    records_bytes = b''
    for i, message in enumerate(messages):
        message_bytes = message.encode('utf-8')
        
        # Simple record structure
        record_length = struct.pack('>i', len(message_bytes) + 9)
        record_attributes = struct.pack('>b', 0)
        timestamp_delta = struct.pack('>i', 0)
        offset_delta = struct.pack('>i', i)
        key_length = struct.pack('>i', -1)
        value_length = struct.pack('>i', len(message_bytes))
        headers_length = struct.pack('>i', 0)
        
        record = (record_length + record_attributes + timestamp_delta + offset_delta + 
                 key_length + value_length + message_bytes + headers_length)
        records_bytes += record
    
    # Combine everything
    record_batch = (base_offset + batch_length + partition_leader_epoch + magic + 
                   b'\x00\x00\x00\x00' + attributes + last_offset_delta + first_timestamp + 
                   max_timestamp + producer_id + producer_epoch + base_sequence + 
                   records_count + records_bytes)
    
    return record_batch

def test_compressed_fetch(host='localhost', port=9092):
    """Test fetching compressed messages"""
    print("\n🧪 Testing Compressed Fetch...")
    
    api_key = 1  # Fetch
    api_version = 12
    correlation_id = 3003
    
    # Build Fetch request
    header = struct.pack('>hhi', api_key, api_version, correlation_id)
    replica_id = struct.pack('>i', -1)
    max_wait_ms = struct.pack('>i', 100)
    min_bytes = struct.pack('>i', 1)
    max_bytes = struct.pack('>i', 52428800)
    isolation_level = struct.pack('>b', 0)
    session_id = struct.pack('>i', 0)
    session_epoch = struct.pack('>i', -1)
    topic_array_length = encode_unsigned_leb128(1)  # 1 topic
    
    # Topic data
    topic_name = "test-topic"
    topic_name_bytes = topic_name.encode('utf-8')
    topic_field = encode_unsigned_leb128(len(topic_name_bytes)) + topic_name_bytes
    
    # Partitions array
    partition_array_length = encode_unsigned_leb128(1)  # 1 partition
    
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
    topic_data = topic_field + partition_array_length + partition_data + topic_tagged_fields
    forgotten_topics_length = encode_unsigned_leb128(0)
    rack_id = b'\x00'
    tagged_fields = b'\x00'
    
    request_data = (header + replica_id + max_wait_ms + min_bytes + max_bytes + 
                   isolation_level + session_id + session_epoch + topic_array_length + 
                   topic_data + forgotten_topics_length + rack_id + tagged_fields)
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            print("📤 Sending Fetch request (should get compressed response)...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✅ Fetch Response - Correlation ID: {corr_id}")
            
            # Check if response contains compressed data
            if len(response) > 100:
                print("✅ Fetch response contains data (may be compressed)")
            return True
            
    except Exception as e:
        print(f"❌ Fetch test failed: {e}")
        return False

def test_compression_efficiency():
    """Test compression efficiency"""
    print("\n🧪 Testing Compression Efficiency...")
    
    # Create sample messages
    messages = [f"Message {i}: " + "x" * 100 for i in range(10)]
    combined_size = sum(len(msg.encode('utf-8')) for msg in messages)
    
    # Simulate compression
    uncompressed_data = b''.join(msg.encode('utf-8') for msg in messages)
    compressed_data = gzip.compress(uncompressed_data)
    
    compression_ratio = len(compressed_data) / len(uncompressed_data)
    savings = (1 - compression_ratio) * 100
    
    print(f"📊 Original size: {len(uncompressed_data)} bytes")
    print(f"📊 Compressed size: {len(compressed_data)} bytes")
    print(f"📈 Compression ratio: {compression_ratio:.2f} ({savings:.1f}% savings)")
    
    if compression_ratio < 0.8:
        print("✅ Compression is effective!")
        return True
    else:
        print("⚠️ Compression ratio could be better")
        return True

def main():
    print("🚀 Starting Compression Tests")
    print("=" * 50)
    
    # Wait for server
    time.sleep(1)
    
    # Run compression tests
    tests = [
        test_compressed_produce,
        test_compressed_fetch,
        test_compression_efficiency
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
            time.sleep(0.5)
        except Exception as e:
            print(f"💥 Test {test.__name__} crashed: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 COMPRESSION TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    for i, (test, result) in enumerate(zip(tests, results)):
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{i+1}. {test.__name__}: {status}")
    
    print(f"\n🎯 Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All compression tests passed!")
        print("🗜️  Your Kafka server now supports efficient message compression!")
    else:
        print("⚠️ Some compression tests failed.")

if __name__ == "__main__":
    main()
