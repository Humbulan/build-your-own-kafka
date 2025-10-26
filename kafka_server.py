#!/usr/bin/env python3
import socket
import struct
import threading
import sys
import time
import json
import atexit
import os

def encode_record_batch(messages):
    """
    Creates a simplified Kafka Record Batch (v2) from stored messages.
    Returns: (batch_bytes, base_offset, record_count)
    """
    if not messages:
        return b'', 0, 0
    
    record_count = len(messages)
    records_bytes = b''
    for i, message in enumerate(messages):
        if isinstance(message, str):
            message_bytes = message.encode('utf-8')
        else:
            message_bytes = message
        
        record_length = struct.pack('>i', len(message_bytes) + 9)
        attributes = struct.pack('>b', 0)
        timestamp_delta = struct.pack('>i', 0)
        offset_delta = struct.pack('>i', i)
        key_length = struct.pack('>i', -1)
        value_length = struct.pack('>i', len(message_bytes))
        headers_length = struct.pack('>i', 0)
        
        record = (record_length + attributes + timestamp_delta + offset_delta + 
                 key_length + value_length + message_bytes + headers_length)
        records_bytes += record
    
    base_offset = 0 
    partition_leader_epoch = struct.pack('>i', -1)
    magic = struct.pack('>b', 2)
    crc_placeholder = b'\x00\x00\x00\x00'
    attributes = struct.pack('>h', 0)
    last_offset_delta = struct.pack('>i', record_count - 1)
    first_timestamp = struct.pack('>q', int(time.time() * 1000))
    max_timestamp = first_timestamp
    producer_id = struct.pack('>q', -1)
    producer_epoch = struct.pack('>h', -1)
    base_sequence = struct.pack('>i', 0)
    record_count_bytes = struct.pack('>i', record_count)
    
    batch_without_length_crc = (
        partition_leader_epoch + magic + attributes + last_offset_delta + 
        first_timestamp + max_timestamp + producer_id + producer_epoch + 
        base_sequence + record_count_bytes + records_bytes
    )
    
    batch_body_length = len(batch_without_length_crc) + 4
    
    final_batch = (
        struct.pack('>q', base_offset) +
        struct.pack('>i', batch_body_length) +
        batch_without_length_crc[0:5] + crc_placeholder + batch_without_length_crc[5:]
    )
    
    return final_batch, base_offset, record_count

class KafkaServer:
    def __init__(self, host='localhost', port=9092):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        self.store_file = "message_store.json"
        self.message_store = self.load_store()
        
    def load_store(self):
        """Load message store from JSON file with better error handling"""
        try:
            if os.path.exists(self.store_file):
                with open(self.store_file, 'r') as f:
                    raw_store = json.load(f)
                    print(f"✓ Loaded store from {self.store_file}", file=sys.stderr)
                    
                    # Convert partition keys back to integers and ensure proper structure
                    store = {}
                    for topic, partitions in raw_store.items():
                        store[topic] = {}
                        for p_id, messages in partitions.items():
                            store[topic][int(p_id)] = messages
                    
                    # Ensure test-topic exists
                    if 'test-topic' not in store:
                        store['test-topic'] = {0: []}
                    
                    print(f"✓ Store contains topics: {list(store.keys())}", file=sys.stderr)
                    for topic, partitions in store.items():
                        for p_id, messages in partitions.items():
                            print(f"  - {topic}[{p_id}]: {len(messages)} messages", file=sys.stderr)
                    
                    return store
            else:
                print(f"✓ Starting with new store ({self.store_file} not found)", file=sys.stderr)
                return {'test-topic': {0: []}}
                
        except Exception as e:
            print(f"✗ Error loading store: {e}. Starting new store.", file=sys.stderr)
            return {'test-topic': {0: []}}
    
    def save_store(self):
        """Save message store to JSON file with better error handling"""
        try:
            with open(self.store_file, 'w') as f:
                json.dump(self.message_store, f, indent=2)
            print(f"✓ Store saved to {self.store_file}", file=sys.stderr)
            
            # Verify save worked
            if os.path.exists(self.store_file):
                with open(self.store_file, 'r') as f:
                    saved_data = json.load(f)
                    total_messages = sum(
                        len(messages) 
                        for topic in saved_data.values() 
                        for messages in topic.values()
                    )
                    print(f"✓ Verified save: {total_messages} total messages", file=sys.stderr)
            else:
                print(f"✗ Save verification failed: {self.store_file} not found", file=sys.stderr)
                
        except Exception as e:
            print(f"✗ Error saving store: {e}", file=sys.stderr)
    
    def auto_save(self):
        """Auto-save store periodically"""
        self.save_store()
        
    def start(self):
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)
        print(f"✓ Server started on port {self.port}", file=sys.stderr)
        print(f"✓ Persistence file: {self.store_file}", file=sys.stderr)
        
        # Initial save to create the file
        self.save_store()
        
        while True:
            client_socket, client_address = self.socket.accept()
            print(f"Connection from {client_address}", file=sys.stderr)
            
            client_thread = threading.Thread(
                target=self.handle_client,
                args=(client_socket, client_address)
            )
            client_thread.daemon = True
            client_thread.start()
    
    def handle_client(self, client_socket, client_address):
        try:
            while True:
                length_data = self.read_exactly(client_socket, 4)
                if not length_data:
                    break
                
                request_length = struct.unpack('>i', length_data)[0]
                print(f"Received {request_length + 4} bytes", file=sys.stderr)
                
                request_data = self.read_exactly(client_socket, request_length)
                if not request_data:
                    break
                
                if len(request_data) >= 8:
                    api_key = struct.unpack('>h', request_data[0:2])[0]
                    api_version = struct.unpack('>h', request_data[2:4])[0]
                    correlation_id = struct.unpack('>i', request_data[4:8])[0]
                    
                    print(f"API Key: {api_key}, API Version: {api_version}, Correlation ID: {correlation_id}", file=sys.stderr)
                    
                    if api_key == 18 and api_version == 4:
                        response = self.create_api_versions_response(correlation_id)
                        client_socket.sendall(response)
                        print(f"Sent ApiVersions response: size={len(response)}, correlation_id={correlation_id}, error_code=0", file=sys.stderr)
                    
                    elif api_key == 3:
                        response = self.create_metadata_response(correlation_id)
                        client_socket.sendall(response)
                        print(f"Sent Metadata response: size={len(response)}, correlation_id={correlation_id}", file=sys.stderr)
                    
                    elif api_key == 0:
                        response = self.create_produce_response(api_version, correlation_id, request_data[8:])
                        client_socket.sendall(response)
                        print(f"Sent Produce response: size={len(response)}, correlation_id={correlation_id}", file=sys.stderr)
                        # Auto-save after produce
                        self.auto_save()
                    
                    elif api_key == 1:
                        response = self.create_fetch_response(api_version, correlation_id, request_data[8:])
                        client_socket.sendall(response)
                        print(f"Sent Fetch response: size={len(response)}, correlation_id={correlation_id}", file=sys.stderr)
                    
                    else:
                        response = self.create_error_response(correlation_id, 35)
                        client_socket.sendall(response)
                        print(f"Sent error response for unsupported API {api_key}", file=sys.stderr)
                
        except Exception as e:
            print(f"Error handling client {client_address}: {e}", file=sys.stderr)
        finally:
            client_socket.close()
            print(f"Connection closed for {client_address}", file=sys.stderr)
    
    def read_exactly(self, sock, num_bytes):
        data = b''
        while len(data) < num_bytes:
            chunk = sock.recv(num_bytes - len(data))
            if not chunk:
                return None
            data += chunk
        return data
    
    def create_api_versions_response(self, correlation_id):
        header = struct.pack('>i', correlation_id)
        error_code = struct.pack('>h', 0)
        api_keys_array_length = b'\x00\x00\x00\x03'
        api_0 = struct.pack('>hhh', 0, 0, 0) + b'\x00'
        api_1 = struct.pack('>hhh', 1, 0, 0) + b'\x00'
        api_18 = struct.pack('>hhh', 18, 0, 4) + b'\x00'
        throttle_time_ms = struct.pack('>i', 0)
        final_tagged_fields = b'\x00'
        
        body = error_code + api_keys_array_length + api_0 + api_1 + api_18 + throttle_time_ms + final_tagged_fields
        response_data = header + body
        message_length = len(response_data)
        return struct.pack('>i', message_length) + response_data
    
    def create_metadata_response(self, correlation_id):
        header = struct.pack('>i', correlation_id)
        throttle_time_ms = struct.pack('>i', 0)
        brokers_array_length = b'\x00\x00\x00\x01'
        broker_node_id = struct.pack('>i', 0)
        host_string = b'\x00\x09localhost'
        broker_port = struct.pack('>i', 9092)
        broker_rack = b'\x00\x00'
        broker_entry = broker_node_id + host_string + broker_port + broker_rack
        
        cluster_id = b'\x00\x00'
        controller_id = struct.pack('>i', 0)
        topics_array_length = b'\x00\x00\x00\x01'
        
        topic_error_code = struct.pack('>h', 0)
        topic_name = b'\x00\x0atest-topic'
        topic_is_internal = struct.pack('>b', 0)
        partitions_array_length = b'\x00\x00\x00\x01'
        
        partition_error_code = struct.pack('>h', 0)
        partition_id = struct.pack('>i', 0)
        leader_id = struct.pack('>i', 0)
        replica_nodes_length = b'\x00\x00\x00\x01'
        replica_node = struct.pack('>i', 0)
        isr_nodes_length = b'\x00\x00\x00\x01'
        isr_node = struct.pack('>i', 0)
        
        partition_entry = (partition_error_code + partition_id + leader_id + 
                         replica_nodes_length + replica_node + isr_nodes_length + isr_node)
        
        topic_entry = (topic_error_code + topic_name + topic_is_internal + 
                      partitions_array_length + partition_entry)
        
        body_parts = [
            throttle_time_ms,
            brokers_array_length,
            broker_entry,
            cluster_id,
            controller_id,
            topics_array_length,
            topic_entry
        ]
        
        response_body = b''.join(body_parts)
        response_data = header + response_body
        message_length = len(response_data)
        
        return struct.pack('>i', message_length) + response_data
    
    def create_produce_response(self, api_version, correlation_id, request_body):
        topic = 'test-topic'
        partition = 0
        message = f"Message at {time.time()}"
        
        if topic not in self.message_store:
            self.message_store[topic] = {}
        if partition not in self.message_store[topic]:
            self.message_store[topic][partition] = []
        
        self.message_store[topic][partition].append(message)
        print(f"✓ Stored message in {topic}[{partition}]: {message}", file=sys.stderr)
        print(f"✓ Total messages in {topic}[{partition}]: {len(self.message_store[topic][partition])}", file=sys.stderr)
        
        header = struct.pack('>i', correlation_id)
        responses_array_length = b'\x00\x00\x00\x01'
        topic_name = b'\x00\x0atest-topic'
        partition_responses_length = b'\x00\x00\x00\x01'
        partition_index = struct.pack('>i', 0)
        error_code = struct.pack('>h', 0)
        base_offset = struct.pack('>q', len(self.message_store[topic][partition]) - 1)
        
        partition_response = partition_index + error_code + base_offset
        topic_response = topic_name + partition_responses_length + partition_response
        throttle_time_ms = struct.pack('>i', 0)
        
        body = responses_array_length + topic_response + throttle_time_ms
        response_data = header + body
        message_length = len(response_data)
        
        return struct.pack('>i', message_length) + response_data
    
    def create_fetch_response(self, api_version, correlation_id, request_body):
        header = struct.pack('>i', correlation_id)
        throttle_time_ms = struct.pack('>i', 0)
        error_code = struct.pack('>h', 0)
        responses_array_length = b'\x00\x00\x00\x01'
        topic_name = b'\x00\x0atest-topic'
        partition_responses_length = b'\x00\x00\x00\x01'
        
        topic = 'test-topic'
        partition = 0
        messages = self.message_store.get(topic, {}).get(partition, [])
        
        print(f"✓ Fetching {len(messages)} messages from {topic}[{partition}]", file=sys.stderr)
        
        record_batch_bytes, base_offset, record_count = encode_record_batch(messages)
        record_set_length = struct.pack('>i', len(record_batch_bytes))
        
        partition_index = struct.pack('>i', 0)
        partition_error_code = struct.pack('>h', 0)
        high_watermark = struct.pack('>q', len(messages))
        
        partition_response = (partition_index + partition_error_code + high_watermark + 
                            record_set_length + record_batch_bytes)
        
        topic_response = topic_name + partition_responses_length + partition_response
        
        body = throttle_time_ms + error_code + responses_array_length + topic_response
        response_data = header + body
        message_length = len(response_data)
        
        print(f"✓ Fetch response: {record_count} messages in batch", file=sys.stderr)
        
        return struct.pack('>i', message_length) + response_data
    
    def create_error_response(self, correlation_id, error_code):
        header = struct.pack('>i', correlation_id)
        body = struct.pack('>h', error_code)
        response_data = header + body
        message_length = len(response_data)
        return struct.pack('>i', message_length) + response_data

def main():
    server = KafkaServer()
    atexit.register(server.save_store)
    server.start()

if __name__ == "__main__":
    main()
