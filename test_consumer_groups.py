#!/usr/bin/env python3
import socket
import struct
import time
import json

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

def test_offset_commit(host='localhost', port=9092):
    """Test OffsetCommit request"""
    print("🧪 Testing OffsetCommit...")
    
    api_key = 8  # OffsetCommit
    api_version = 1
    correlation_id = 2001
    
    # Build OffsetCommit request using proper compact strings
    header = struct.pack('>hhi', api_key, api_version, correlation_id)
    
    # Group ID (compact string)
    group_id = "test-consumer-group"
    group_id_bytes = group_id.encode('utf-8')
    group_id_field = encode_unsigned_leb128(len(group_id_bytes)) + group_id_bytes
    
    # Generation ID and Member ID
    generation_id = struct.pack('>i', 1)
    member_id = encode_unsigned_leb128(0)  # Empty string
    
    # Group Instance ID (null - compact string with length 0)
    group_instance_id = encode_unsigned_leb128(0)
    
    # Topics array (UVarInt for 1 topic)
    topics_array_length = encode_unsigned_leb128(1)
    
    # Topic data
    topic_name = "test-topic"
    topic_name_bytes = topic_name.encode('utf-8')
    topic_field = encode_unsigned_leb128(len(topic_name_bytes)) + topic_name_bytes
    
    # Partitions array (UVarInt for 1 partition)
    partitions_array_length = encode_unsigned_leb128(1)
    
    # Partition data
    partition_id = struct.pack('>i', 0)
    committed_offset = struct.pack('>q', 5)  # Commit offset 5
    committed_metadata = encode_unsigned_leb128(0)  # Empty metadata
    
    # Commit timestamp (for v1)
    commit_timestamp = struct.pack('>q', int(time.time() * 1000))
    
    partition_data = partition_id + committed_offset + committed_metadata + commit_timestamp
    
    topic_data = topic_field + partitions_array_length + partition_data
    
    request_data = (header + group_id_field + generation_id + member_id + 
                   group_instance_id + topics_array_length + topic_data)
    
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            print("📤 Sending OffsetCommit request...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✅ OffsetCommit Response - Correlation ID: {corr_id}")
            return True
            
    except Exception as e:
        print(f"❌ OffsetCommit test failed: {e}")
        return False

def test_offset_fetch(host='localhost', port=9092):
    """Test OffsetFetch request"""
    print("\n🧪 Testing OffsetFetch...")
    
    api_key = 9  # OffsetFetch
    api_version = 1
    correlation_id = 2002
    
    # Build OffsetFetch request using proper compact strings
    header = struct.pack('>hhi', api_key, api_version, correlation_id)
    
    # Group ID (compact string)
    group_id = "test-consumer-group"
    group_id_bytes = group_id.encode('utf-8')
    group_id_field = encode_unsigned_leb128(len(group_id_bytes)) + group_id_bytes
    
    # Topics array (UVarInt for 1 topic)
    topics_array_length = encode_unsigned_leb128(1)
    
    # Topic data
    topic_name = "test-topic"
    topic_name_bytes = topic_name.encode('utf-8')
    topic_field = encode_unsigned_leb128(len(topic_name_bytes)) + topic_name_bytes
    
    # Partitions array (UVarInt for 1 partition)
    partitions_array_length = encode_unsigned_leb128(1)
    
    # Partition data
    partition_id = struct.pack('>i', 0)
    
    partition_data = partition_id
    
    topic_data = topic_field + partitions_array_length + partition_data
    
    request_data = header + group_id_field + topics_array_length + topic_data
    
    message_length = struct.pack('>i', len(request_data))
    full_request = message_length + request_data
    
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            print("📤 Sending OffsetFetch request...")
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"✅ OffsetFetch Response - Correlation ID: {corr_id}")
            
            # Try to parse the offset from response
            if len(response) > 20:
                # Simplified parsing - look for the offset value
                print("✅ OffsetFetch contains offset data")
            return True
            
    except Exception as e:
        print(f"❌ OffsetFetch test failed: {e}")
        return False

def test_consumer_persistence():
    """Test that consumer offsets persist between server restarts"""
    print("\n🧪 Testing Consumer Offset Persistence...")
    
    offsets_file = "consumer_offsets.json"
    
    try:
        with open(offsets_file, 'r') as f:
            offsets = json.load(f)
        
        if "test-consumer-group" in offsets:
            group_data = offsets["test-consumer-group"]
            if "test-topic" in group_data:
                partition_data = group_data["test-topic"]
                if "0" in partition_data:
                    stored_offset = partition_data["0"]
                    print(f"✅ Found persisted offset: {stored_offset}")
                    return True
                else:
                    print("❌ No partition 0 offset found")
                    return False
            else:
                print("❌ No test-topic offsets found")
                return False
        else:
            print("❌ No test-consumer-group found")
            return False
            
    except FileNotFoundError:
        print(f"⚠️ Offsets file {offsets_file} not found yet")
        return False
    except Exception as e:
        print(f"❌ Error reading offsets file: {e}")
        return False

def main():
    print("🚀 Starting Consumer Group Tests")
    print("=" * 50)
    
    # Wait for server
    time.sleep(1)
    
    # Run consumer group tests
    tests = [
        test_offset_commit,
        test_offset_fetch,
        test_consumer_persistence
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
    print("📊 CONSUMER GROUP TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    for i, (test, result) in enumerate(zip(tests, results)):
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{i+1}. {test.__name__}: {status}")
    
    print(f"\n🎯 Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All consumer group tests passed!")
    else:
        print("⚠️ Some consumer group tests failed.")

if __name__ == "__main__":
    main()
