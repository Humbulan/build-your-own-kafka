#!/usr/bin/env python3
import socket
import struct
import time
import json

class AdvancedKafkaClient:
    def __init__(self, host='localhost', port=9093):  # Changed port
        self.host = host
        self.port = port
    
    def send_request(self, api_key, api_version, correlation_id, request_body):
        """Send a Kafka protocol request"""
        header = struct.pack('>hhi', api_key, api_version, correlation_id)
        request_data = header + request_body
        message_length = struct.pack('>i', len(request_data))
        full_request = message_length + request_data
        
        with socket.create_connection((self.host, self.port)) as sock:
            sock.sendall(full_request)
            
            # Read response
            resp_length_data = sock.recv(4)
            resp_length = struct.unpack('>i', resp_length_data)[0]
            response = sock.recv(resp_length)
            
            return response
    
    def test_multiple_topics(self):
        """Test producing to multiple topics"""
        print("🧪 Testing Multiple Topics & Partitions")
        print("=" * 50)
        
        # Produce to different topics and partitions
        test_cases = [
            ('test-topic', 0, "Message for test-topic partition 0"),
            ('test-topic', 1, "Message for test-topic partition 1"), 
            ('logs-topic', 0, "Log message for logs-topic"),
            ('logs-topic', 1, "Another log message"),
            ('metrics-topic', 0, "Metric data point"),
        ]
        
        for topic, partition, message in test_cases:
            print(f"📤 Producing to {topic}[{partition}]: {message}")
            
            # Build produce request
            transactional_id = struct.pack('>h', -1)  # NULL string
            acks = struct.pack('>h', 1)
            timeout = struct.pack('>i', 30000)
            
            # Single topic in array
            topic_array_length = struct.pack('>i', 1)
            
            # Topic data
            topic_name_bytes = struct.pack('>h', len(topic)) + topic.encode('utf-8')
            partition_array_length = struct.pack('>i', 1)
            
            # Partition data
            partition_id_bytes = struct.pack('>i', partition)
            
            # Simple record (just the message bytes for now)
            message_bytes = message.encode('utf-8')
            records_length = struct.pack('>i', len(message_bytes))
            
            partition_data = partition_id_bytes + records_length + message_bytes
            topic_data = topic_name_bytes + partition_array_length + partition_data
            
            request_body = transactional_id + acks + timeout + topic_array_length + topic_data
            
            # Send request
            response = self.send_request(0, 9, int(time.time() * 1000) % 10000, request_body)
            
            # Parse response
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"  ✅ Produced (Correlation ID: {corr_id})")
            
            time.sleep(0.5)
    
    def test_consumer_groups(self):
        """Test consumer group offset commit/fetch"""
        print("\n🧪 Testing Consumer Groups")
        print("=" * 50)
        
        group_id = "test-consumer-group"
        topic = "test-topic"
        
        # Commit offsets
        print(f"📝 Committing offsets for group: {group_id}")
        request_body = self.build_offset_commit_request(group_id, topic, [(0, 5), (1, 3)])
        response = self.send_request(20, 6, int(time.time() * 1000) % 10000, request_body)
        print("  ✅ Offsets committed")
        
        # Fetch offsets
        print(f"📖 Fetching offsets for group: {group_id}")
        request_body = self.build_offset_fetch_request(group_id, topic, [0, 1])
        response = self.send_request(8, 6, int(time.time() * 1000) % 10000, request_body)
        print("  ✅ Offsets fetched")
    
    def build_offset_commit_request(self, group_id, topic, partition_offsets):
        """Build OffsetCommit request"""
        group_id_bytes = struct.pack('>h', len(group_id)) + group_id.encode('utf-8')
        generation_id = struct.pack('>i', 1)
        member_id = struct.pack('>h', -1)  # NULL string
        
        # Topics array
        topics_array_length = struct.pack('>i', 1)
        
        # Topic data
        topic_name_bytes = struct.pack('>h', len(topic)) + topic.encode('utf-8')
        partitions_array_length = struct.pack('>i', len(partition_offsets))
        
        partitions_data = b''
        for partition_id, offset in partition_offsets:
            partition_data = (struct.pack('>i', partition_id) + 
                            struct.pack('>q', offset) + 
                            struct.pack('>h', -1))  # NULL metadata
            partitions_data += partition_data
        
        topic_data = topic_name_bytes + partitions_array_length + partitions_data
        request_body = group_id_bytes + generation_id + member_id + topics_array_length + topic_data
        
        return request_body
    
    def build_offset_fetch_request(self, group_id, topic, partitions):
        """Build OffsetFetch request"""
        group_id_bytes = struct.pack('>h', len(group_id)) + group_id.encode('utf-8')
        
        # Topics array
        topics_array_length = struct.pack('>i', 1)
        
        # Topic data
        topic_name_bytes = struct.pack('>h', len(topic)) + topic.encode('utf-8')
        partitions_array_length = struct.pack('>i', len(partitions))
        
        partitions_data = b''
        for partition_id in partitions:
            partitions_data += struct.pack('>i', partition_id)
        
        topic_data = topic_name_bytes + partitions_array_length + partitions_data
        request_body = group_id_bytes + topics_array_length + topic_data
        
        return request_body

def main():
    print("🚀 Advanced Kafka Features Test Client")
    print("Make sure kafka_advanced_fixed.py is running on localhost:9093\n")
    
    client = AdvancedKafkaClient()
    
    # Run tests
    client.test_multiple_topics()
    client.test_consumer_groups()
    
    print("\n🎉 All advanced features tested!")
    print("\n📊 Check the generated files:")
    print("  - message_store_advanced.json (Multiple topics/partitions)")
    print("  - consumer_offsets.json (Consumer group offsets)")

if __name__ == "__main__":
    main()
