#!/usr/bin/env python3
import socket
import struct
import threading
import sys
import time
import json
import atexit
import os
import signal
import gzip
import zlib

def decode_unsigned_leb128(data, offset):
    """Decode unsigned LEB128 variable-length integer"""
    result = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
    return result, offset

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

def compress_record_batch(record_batch_bytes, compression_type):
    """Compress record batch using specified compression type"""
    if compression_type == 1:  # GZIP
        return gzip.compress(record_batch_bytes)
    elif compression_type == 2:  # Snappy (simplified - using zlib for demo)
        # Note: Real Snappy would use python-snappy library
        return zlib.compress(record_batch_bytes, level=1)
    elif compression_type == 3:  # LZ4 (simplified - using zlib for demo)
        return zlib.compress(record_batch_bytes, level=1)
    else:  # No compression
        return record_batch_bytes

def encode_record_batch(messages, compression_type=0):
    """
    Creates a simplified Kafka Record Batch (v2) from stored messages.
    Returns: (batch_bytes, base_offset, record_count)
    """
    if not messages:
        # Return empty bytes and offsets if no messages
        return b'', 0, 0
    
    record_count = len(messages)
    
    # 1. Build individual records
    records_bytes = b''
    for i, message in enumerate(messages):
        # Convert message to bytes if it's a string
        if isinstance(message, str):
            message_bytes = message.encode('utf-8')
        else:
            message_bytes = message
        
        # Record structure
        record_length = struct.pack('>i', len(message_bytes) + 9)  # +9 for header
        attributes = struct.pack('>b', 0)  # No compression at record level
        timestamp_delta = struct.pack('>i', 0)  # No timestamp delta
        offset_delta = struct.pack('>i', i)  # Offset within batch
        key_length = struct.pack('>i', -1)  # No key
        value_length = struct.pack('>i', len(message_bytes))  # Value length
        headers_length = struct.pack('>i', 0)  # No headers
        
        record = (record_length + attributes + timestamp_delta + offset_delta + 
                 key_length + value_length + message_bytes + headers_length)
        records_bytes += record
    
    # 2. Batch Header Fields
    base_offset = 0 
    partition_leader_epoch = struct.pack('>i', -1)
    magic = struct.pack('>b', 2)  # Record batch v2
    
    # Set compression in attributes
    attributes = struct.pack('>h', compression_type << 3)  # Compression bits: 5-7
    
    last_offset_delta = struct.pack('>i', record_count - 1)
    first_timestamp = struct.pack('>q', int(time.time() * 1000))
    max_timestamp = first_timestamp  # Same as first for simplicity
    producer_id = struct.pack('>q', -1)
    producer_epoch = struct.pack('>h', -1)
    base_sequence = struct.pack('>i', 0)
    record_count_bytes = struct.pack('>i', record_count)
    
    # Batch body without length and CRC
    batch_without_length_crc = (
        partition_leader_epoch + magic + attributes + last_offset_delta + 
        first_timestamp + max_timestamp + producer_id + producer_epoch + 
        base_sequence + record_count_bytes + records_bytes
    )
    
    # Apply compression if requested
    if compression_type > 0:
        compressed_records = compress_record_batch(records_bytes, compression_type)
        # Rebuild batch with compressed records
        batch_without_length_crc = (
            partition_leader_epoch + magic + attributes + last_offset_delta + 
            first_timestamp + max_timestamp + producer_id + producer_epoch + 
            base_sequence + record_count_bytes + compressed_records
        )
    
    # 3. Calculate CRC (simplified - using placeholder)
    crc_placeholder = b'\x00\x00\x00\x00'  # In production, calculate actual CRC
    
    # 4. Calculate Final Length
    batch_body_length = len(batch_without_length_crc) + 4  # +4 for CRC
    
    # Final Batch (BaseOffset + BatchLength + Body)
    final_batch = (
        struct.pack('>q', base_offset) +  # Base offset
        struct.pack('>i', batch_body_length) +  # Batch length
        batch_without_length_crc[0:5] + crc_placeholder + batch_without_length_crc[5:]
    )
    
    return final_batch, base_offset, record_count

def decompress_record_batch(compressed_data, compression_type):
    """Decompress record batch using specified compression type"""
    if compression_type == 1:  # GZIP
        return gzip.decompress(compressed_data)
    elif compression_type == 2:  # Snappy (simplified)
        return zlib.decompress(compressed_data)
    elif compression_type == 3:  # LZ4 (simplified)
        return zlib.decompress(compressed_data)
    else:  # No compression
        return compressed_data

class KafkaServer:
    def __init__(self, host='localhost', port=9092):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Message storage with persistence
        self.store_file = "kafka_persistent_store.json"
        self.message_store = self.load_store()
        
        # Consumer offsets storage with persistence
        self.offsets_file = "consumer_offsets.json"
        self.consumer_offsets = self.load_offsets()
        
        self.shutdown_requested = False
        
    def load_store(self):
        """Load message store from JSON file"""
        try:
            with open(self.store_file, 'r') as f:
                raw_store = json.load(f)
                # Convert partition keys back to integers
                return {
                    topic: {int(p_id): messages for p_id, messages in partitions.items()}
                    for topic, partitions in raw_store.items()
                }
        except FileNotFoundError:
            print(f"[{self.store_file}] not found. Starting with new store.", file=sys.stderr)
            return {'test-topic': {0: []}}
        except Exception as e:
            print(f"Error loading store: {e}. Starting new store.", file=sys.stderr)
            return {'test-topic': {0: []}}
    
    def load_offsets(self):
        """Load consumer offsets from JSON file"""
        try:
            with open(self.offsets_file, 'r') as f:
                raw_offsets = json.load(f)
                # Convert nested keys back to integers
                result = {}
                for group_id, topics in raw_offsets.items():
                    result[group_id] = {}
                    for topic_name, partitions in topics.items():
                        result[group_id][topic_name] = {int(p_id): offset for p_id, offset in partitions.items()}
                return result
        except FileNotFoundError:
            print(f"[{self.offsets_file}] not found. Starting with new offsets store.", file=sys.stderr)
            return {}
        except Exception as e:
            print(f"Error loading offsets: {e}. Starting new offsets store.", file=sys.stderr)
            return {}
    
    def save_store(self):
        """Save message store to JSON file"""
        try:
            with open(self.store_file, 'w') as f:
                json.dump(self.message_store, f, indent=2)
            message_count = sum(len(partitions.get(0, [])) for partitions in self.message_store.values())
            print(f"💾 Store saved to [{self.store_file}] with {message_count} messages", file=sys.stderr)
        except Exception as e:
            print(f"❌ Error saving store: {e}", file=sys.stderr)
    
    def save_offsets(self):
        """Save consumer offsets to JSON file"""
        try:
            with open(self.offsets_file, 'w') as f:
                json.dump(self.consumer_offsets, f, indent=2)
            group_count = len(self.consumer_offsets)
            offset_count = sum(len(partitions) for group in self.consumer_offsets.values() 
                             for partitions in group.values())
            print(f"💾 Offsets saved to [{self.offsets_file}] with {offset_count} offsets across {group_count} groups", file=sys.stderr)
        except Exception as e:
            print(f"❌ Error saving offsets: {e}", file=sys.stderr)
    
    def save_all(self):
        """Save both message store and offsets"""
        self.save_store()
        self.save_offsets()
    
    def graceful_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print(f"\n✋ Received signal {signum}. Shutting down gracefully...", file=sys.stderr)
        self.shutdown_requested = True
        self.save_all()
        print("✅ State saved. Exiting.", file=sys.stderr)
        # Close the server socket to break out of the accept() loop
        try:
            self.socket.close()
        except:
            pass
        sys.exit(0)
        
    def start(self):
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.graceful_shutdown)
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)
        print(f"🚀 Enhanced Kafka Server started on port {self.port}", file=sys.stderr)
        print(f"💾 Persistence file: {self.store_file}", file=sys.stderr)
        print(f"📊 Message store: {len(self.message_store.get('test-topic', {}).get(0, []))} messages", file=sys.stderr)
        print(f"📍 Offsets file: {self.offsets_file}", file=sys.stderr)
        print(f"👥 Consumer groups: {len(self.consumer_offsets)} groups", file=sys.stderr)
        print("🗜️  Compression: GZIP supported", file=sys.stderr)
        print("⏹️  Use Ctrl+C or 'kill' to shutdown gracefully", file=sys.stderr)
        
        # Set socket timeout to allow checking for shutdown
        self.socket.settimeout(1.0)
        
        while not self.shutdown_requested:
            try:
                client_socket, client_address = self.socket.accept()
                print(f"🔗 Connection from {client_address}", file=sys.stderr)
                
                # Handle each client connection in a separate thread
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address)
                )
                client_thread.daemon = True
                client_thread.start()
                
            except socket.timeout:
                # Timeout occurred, check if we should shutdown
                continue
            except OSError:
                # Socket was closed, likely during shutdown
                if not self.shutdown_requested:
                    raise
        
        print("🛑 Server shutdown complete", file=sys.stderr)
    
    def handle_client(self, client_socket, client_address):
        """Handle multiple sequential requests from a single client"""
        try:
            while not self.shutdown_requested:
                # Read the request length (first 4 bytes)
                length_data = self.read_exactly(client_socket, 4)
                if not length_data:
                    break  # Client disconnected
                
                request_length = struct.unpack('>i', length_data)[0]
                print(f"📨 Received {request_length + 4} bytes", file=sys.stderr)
                
                # Read the complete request
                request_data = self.read_exactly(client_socket, request_length)
                if not request_data:
                    break
                
                # Parse request header
                if len(request_data) >= 8:
                    api_key = struct.unpack('>h', request_data[0:2])[0]
                    api_version = struct.unpack('>h', request_data[2:4])[0]
                    correlation_id = struct.unpack('>i', request_data[4:8])[0]
                    
                    print(f"🔧 API Key: {api_key}, API Version: {api_version}, Correlation ID: {correlation_id}", file=sys.stderr)
                    
                    if api_key == 18 and api_version == 4:
                        # ApiVersions request
                        response = self.create_api_versions_response(correlation_id)
                        client_socket.sendall(response)
                        print(f"📤 Sent ApiVersions response: size={len(response)}, correlation_id={correlation_id}, error_code=0", file=sys.stderr)
                    
                    elif api_key == 3:
                        # Metadata request
                        response = self.create_metadata_response(correlation_id)
                        client_socket.sendall(response)
                        print(f"📤 Sent Metadata response: size={len(response)}, correlation_id={correlation_id}", file=sys.stderr)
                    
                    elif api_key == 0:
                        # Produce request
                        response = self.create_produce_response(api_version, correlation_id, request_data[8:])
                        client_socket.sendall(response)
                        print(f"📤 Sent Produce response: size={len(response)}, correlation_id={correlation_id}", file=sys.stderr)
                    
                    elif api_key == 1:
                        # Fetch request
                        response = self.create_fetch_response(api_version, correlation_id, request_data[8:])
                        client_socket.sendall(response)
                        print(f"📤 Sent Fetch response: size={len(response)}, correlation_id={correlation_id}", file=sys.stderr)
                    
                    elif api_key == 8:
                        # OffsetCommit request (Consumer Group)
                        response = self.create_offset_commit_response(api_version, correlation_id, request_data[8:])
                        client_socket.sendall(response)
                        print(f"📤 Sent OffsetCommit response: size={len(response)}, correlation_id={correlation_id}", file=sys.stderr)
                    
                    elif api_key == 9:
                        # OffsetFetch request (Consumer Group)
                        response = self.create_offset_fetch_response(api_version, correlation_id, request_data[8:])
                        client_socket.sendall(response)
                        print(f"📤 Sent OffsetFetch response: size={len(response)}, correlation_id={correlation_id}", file=sys.stderr)
                    
                    else:
                        # Unsupported API
                        response = self.create_error_response(correlation_id, 35)
                        client_socket.sendall(response)
                        print(f"❌ Sent error response for unsupported API {api_key}", file=sys.stderr)
                
        except Exception as e:
            if not self.shutdown_requested:
                print(f"💥 Error handling client {client_address}: {e}", file=sys.stderr)
        finally:
            client_socket.close()
            if not self.shutdown_requested:
                print(f"🔒 Connection closed for {client_address}", file=sys.stderr)
    
    def read_exactly(self, sock, num_bytes):
        """Read exactly num_bytes from the socket"""
        data = b''
        while len(data) < num_bytes and not self.shutdown_requested:
            try:
                sock.settimeout(1.0)
                chunk = sock.recv(num_bytes - len(data))
                if not chunk:
                    return None
                data += chunk
            except socket.timeout:
                if self.shutdown_requested:
                    return None
                continue
        return data
    
    def create_api_versions_response(self, correlation_id):
        """Create ApiVersions response for API key 18"""
        header = struct.pack('>i', correlation_id)
        error_code = struct.pack('>h', 0)
        api_keys_array_length = struct.pack('>i', 5)[-1:]  # UVarInt for 5 APIs
        api_0 = struct.pack('>hhh', 0, 0, 0) + b'\x00'
        api_1 = struct.pack('>hhh', 1, 0, 0) + b'\x00'
        api_8 = struct.pack('>hhh', 8, 0, 0) + b'\x00'   # OffsetCommit
        api_9 = struct.pack('>hhh', 9, 0, 0) + b'\x00'   # OffsetFetch
        api_18 = struct.pack('>hhh', 18, 0, 4) + b'\x00'
        throttle_time_ms = struct.pack('>i', 0)
        final_tagged_fields = b'\x00'
        
        body = error_code + api_keys_array_length + api_0 + api_1 + api_8 + api_9 + api_18 + throttle_time_ms + final_tagged_fields
        response_data = header + body
        message_length = len(response_data)
        return struct.pack('>i', message_length) + response_data
    
    def create_metadata_response(self, correlation_id):
        """Create Metadata response for API key 3 (Version 9 - Flexible)"""
        # Response header
        header = struct.pack('>i', correlation_id)
        
        # Response body
        throttle_time_ms = struct.pack('>i', 0)
        brokers_array_length = struct.pack('>i', 1)[-1:]  # UVarInt for 1 broker
        
        # Broker Entry
        broker_node_id = struct.pack('>i', 0)
        host_string = b'\x09' + b'localhost'  # "localhost" as compact string
        broker_port = struct.pack('>i', 9092)
        broker_rack = b'\x00'
        broker_tagged_fields = b'\x00'
        broker_entry = broker_node_id + host_string + broker_port + broker_rack + broker_tagged_fields
        
        cluster_id = b'\x00'
        controller_id = struct.pack('>i', -1)
        topics_array_length = struct.pack('>i', 1)[-1:]  # UVarInt for 1 topic
        
        # Topic Entry
        topic_error_code = struct.pack('>h', 0)
        topic_name = b'\x0a' + b'test-topic'  # "test-topic" as compact string
        topic_id = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'  # UUID NULL
        topic_is_internal = b'\x00'  # False
        partitions_array_length = struct.pack('>i', 1)[-1:]  # UVarInt for 1 partition
        
        # Partition Entry
        partition_error_code = struct.pack('>h', 0)
        partition_id = struct.pack('>i', 0)
        leader_id = struct.pack('>i', 0)
        leader_epoch = struct.pack('>i', -1)
        replica_nodes_length = struct.pack('>i', 1)[-1:]  # UVarInt for 1 replica
        replica_node = struct.pack('>i', 0)
        isr_nodes_length = struct.pack('>i', 1)[-1:]  # UVarInt for 1 ISR
        isr_node = struct.pack('>i', 0)
        offline_replicas_length = struct.pack('>i', 0)[-1:]  # UVarInt for 0 offline replicas
        partition_tagged_fields = b'\x00'
        
        partition_entry = (partition_error_code + partition_id + leader_id + leader_epoch + 
                         replica_nodes_length + replica_node + isr_nodes_length + isr_node + 
                         offline_replicas_length + partition_tagged_fields)
        
        topic_tagged_fields = b'\x00'
        
        topic_entry = (topic_error_code + topic_name + topic_id + topic_is_internal + 
                      partitions_array_length + partition_entry + topic_tagged_fields)
        
        final_tagged_fields = b'\x00'
        
        # Build complete response body
        body_parts = [
            throttle_time_ms,
            brokers_array_length,
            broker_entry,
            cluster_id,
            controller_id,
            topics_array_length,
            topic_entry,
            final_tagged_fields
        ]
        
        response_body = b''.join(body_parts)
        response_data = header + response_body
        message_length = len(response_data)
        
        return struct.pack('>i', message_length) + response_data
    
    def create_produce_response(self, api_version, correlation_id, request_body):
        """Create Produce response for API key 0"""
        # For this demo, we'll just store a simple message
        # In a real implementation, you'd parse the compressed record batch
        
        topic = 'test-topic'
        partition = 0
        message = f"Message at {time.time()}"
        
        # Store the message
        if topic not in self.message_store:
            self.message_store[topic] = {}
        if partition not in self.message_store[topic]:
            self.message_store[topic][partition] = []
        
        self.message_store[topic][partition].append(message)
        print(f"💾 Stored message in {topic}[{partition}]: {message}", file=sys.stderr)
        
        # Build Produce Response V9
        header = struct.pack('>i', correlation_id)
        
        # Responses array (1 topic)
        responses_array_length = struct.pack('>i', 1)[-1:]  # UVarInt for 1
        
        # Topic response
        topic_name = b'\x0a' + b'test-topic'  # "test-topic" as compact string
        
        # Partition responses array (1 partition)
        partition_responses_length = struct.pack('>i', 1)[-1:]  # UVarInt for 1
        
        # Partition response
        partition_index = struct.pack('>i', 0)
        error_code = struct.pack('>h', 0)  # NO_ERROR
        base_offset = struct.pack('>q', len(self.message_store[topic][partition]) - 1)  # Last offset
        log_append_time = struct.pack('>q', -1)  # -1 for unknown
        log_start_offset = struct.pack('>q', 0)  # Start offset
        record_errors_length = struct.pack('>i', 0)[-1:]  # UVarInt for 0 record errors
        error_message = b'\x00'  # NULL compact string
        partition_response_tagged_fields = b'\x00'
        
        partition_response = (partition_index + error_code + base_offset + log_append_time + 
                            log_start_offset + record_errors_length + error_message + 
                            partition_response_tagged_fields)
        
        topic_response_tagged_fields = b'\x00'
        
        topic_response = topic_name + partition_responses_length + partition_response + topic_response_tagged_fields
        
        throttle_time_ms = struct.pack('>i', 0)
        final_tagged_fields = b'\x00'
        
        body = (responses_array_length + topic_response + throttle_time_ms + 
               final_tagged_fields)
        response_data = header + body
        message_length = len(response_data)
        
        return struct.pack('>i', message_length) + response_data
    
    def create_fetch_response(self, api_version, correlation_id, request_body):
        """Create Fetch response for API key 1 with compression support"""
        header = struct.pack('>i', correlation_id)
        
        throttle_time_ms = struct.pack('>i', 0)
        error_code = struct.pack('>h', 0)  # NO_ERROR
        session_id = struct.pack('>i', 0)
        
        # Responses array (1 topic)
        responses_array_length = struct.pack('>i', 1)[-1:]  # UVarInt for 1
        
        # Topic response
        topic_name = b'\x0a' + b'test-topic'  # "test-topic" as compact string
        
        # Partition responses array (1 partition)
        partition_responses_length = struct.pack('>i', 1)[-1:]  # UVarInt for 1
        
        # Get stored messages and encode them
        topic = 'test-topic'
        partition = 0
        messages = self.message_store.get(topic, {}).get(partition, [])
        
        # Use compression for batches with multiple messages
        compression_type = 1 if len(messages) > 1 else 0  # GZIP for multiple messages
        
        # Encode messages into Kafka Record Batch format with optional compression
        record_batch_bytes, base_offset, record_count = encode_record_batch(messages, compression_type)
        record_set_length = struct.pack('>i', len(record_batch_bytes))
        
        # Partition response with actual record data
        partition_index = struct.pack('>i', 0)
        partition_error_code = struct.pack('>h', 0)  # NO_ERROR
        high_watermark = struct.pack('>q', len(messages))
        last_stable_offset = struct.pack('>q', 0)
        log_start_offset = struct.pack('>q', 0)
        aborted_transactions_length = struct.pack('>i', 0)[-1:]  # UVarInt for 0
        preferred_read_replica = struct.pack('>i', -1)  # -1 for not set
        
        partition_response_tagged_fields = b'\x00'
        
        partition_response = (partition_index + partition_error_code + high_watermark + 
                            last_stable_offset + log_start_offset + aborted_transactions_length + 
                            preferred_read_replica + record_set_length + record_batch_bytes + 
                            partition_response_tagged_fields)
        
        topic_response_tagged_fields = b'\x00'
        
        topic_response = topic_name + partition_responses_length + partition_response + topic_response_tagged_fields
        
        final_tagged_fields = b'\x00'
        
        body = (throttle_time_ms + error_code + session_id + responses_array_length + 
               topic_response + final_tagged_fields)
        response_data = header + body
        message_length = len(response_data)
        
        compression_info = " (compressed)" if compression_type > 0 else ""
        print(f"📤 Fetch response: {record_count} messages in batch{compression_info}", file=sys.stderr)
        
        return struct.pack('>i', message_length) + response_data
    
    def create_offset_commit_response(self, api_version, correlation_id, request_body):
        """Create OffsetCommit response for API key 8"""
        print(f"📍 Processing OffsetCommit request", file=sys.stderr)
        
        # For simplicity, let's use a hardcoded approach for v1
        # Group ID (compact string)
        pos = 0
        
        # Group ID length (UVarInt)
        group_id_len, pos = decode_unsigned_leb128(request_body, pos)
        group_id = request_body[pos:pos+group_id_len].decode('utf-8')
        pos += group_id_len
        
        print(f"📍 Group ID: {group_id}", file=sys.stderr)
        
        # Generation ID (int32)
        generation_id = struct.unpack('>i', request_body[pos:pos+4])[0]
        pos += 4
        
        # Member ID (compact string)
        member_id_len, pos = decode_unsigned_leb128(request_body, pos)
        member_id = request_body[pos:pos+member_id_len].decode('utf-8') if member_id_len > 0 else ""
        pos += member_id_len
        
        # Group Instance ID (compact string, can be null)
        group_instance_id_len, pos = decode_unsigned_leb128(request_body, pos)
        if group_instance_id_len > 0:
            group_instance_id = request_body[pos:pos+group_instance_id_len].decode('utf-8')
            pos += group_instance_id_len
        
        # Retention time (int64) - only in v2+, skip for v1
        if api_version >= 2:
            retention_time = struct.unpack('>q', request_body[pos:pos+8])[0]
            pos += 8
        
        # Topics array (UVarInt)
        topics_count, pos = decode_unsigned_leb128(request_body, pos)
        print(f"📍 Topics count: {topics_count}", file=sys.stderr)
        
        # Process each topic
        for topic_idx in range(topics_count):
            # Topic name (compact string)
            topic_name_len, pos = decode_unsigned_leb128(request_body, pos)
            topic_name = request_body[pos:pos+topic_name_len].decode('utf-8')
            pos += topic_name_len
            
            # Partitions array (UVarInt)
            partitions_count, pos = decode_unsigned_leb128(request_body, pos)
            print(f"📍 Topic: {topic_name}, Partitions: {partitions_count}", file=sys.stderr)
            
            # Process each partition
            for partition_idx in range(partitions_count):
                # Partition ID (int32)
                partition_id = struct.unpack('>i', request_body[pos:pos+4])[0]
                pos += 4
                
                # Committed offset (int64)
                committed_offset = struct.unpack('>q', request_body[pos:pos+8])[0]
                pos += 8
                
                # Committed metadata (compact string)
                metadata_len, pos = decode_unsigned_leb128(request_body, pos)
                metadata = request_body[pos:pos+metadata_len].decode('utf-8') if metadata_len > 0 else ""
                pos += metadata_len
                
                # Commit timestamp (int64) - only in v1
                if api_version == 1:
                    commit_timestamp = struct.unpack('>q', request_body[pos:pos+8])[0]
                    pos += 8
                
                # Store the committed offset
                if group_id not in self.consumer_offsets:
                    self.consumer_offsets[group_id] = {}
                if topic_name not in self.consumer_offsets[group_id]:
                    self.consumer_offsets[group_id][topic_name] = {}
                
                self.consumer_offsets[group_id][topic_name][partition_id] = committed_offset
                print(f"📍 Stored offset for {group_id}/{topic_name}[{partition_id}]: {committed_offset}", file=sys.stderr)
        
        # Save offsets immediately
        self.save_offsets()
        
        # Build response
        header = struct.pack('>i', correlation_id)
        throttle_time_ms = struct.pack('>i', 0)
        
        # Topics array (empty in success case for v0-1)
        topics_array_length = encode_unsigned_leb128(0)  # UVarInt for 0
        final_tagged_fields = b'\x00'
        
        body = throttle_time_ms + topics_array_length + final_tagged_fields
        response_data = header + body
        message_length = len(response_data)
        
        return struct.pack('>i', message_length) + response_data
    
    def create_offset_fetch_response(self, api_version, correlation_id, request_body):
        """Create OffsetFetch response for API key 9"""
        print(f"📍 Processing OffsetFetch request", file=sys.stderr)
        
        pos = 0
        
        # Group ID (compact string)
        group_id_len, pos = decode_unsigned_leb128(request_body, pos)
        group_id = request_body[pos:pos+group_id_len].decode('utf-8')
        pos += group_id_len
        
        # Topics array (UVarInt)
        topics_count, pos = decode_unsigned_leb128(request_body, pos)
        
        requested_topics = []
        for topic_idx in range(topics_count):
            # Topic name (compact string)
            topic_name_len, pos = decode_unsigned_leb128(request_body, pos)
            topic_name = request_body[pos:pos+topic_name_len].decode('utf-8')
            pos += topic_name_len
            
            # Partitions array (UVarInt)
            partitions_count, pos = decode_unsigned_leb128(request_body, pos)
            
            requested_partitions = []
            for partition_idx in range(partitions_count):
                # Partition ID (int32)
                partition_id = struct.unpack('>i', request_body[pos:pos+4])[0]
                pos += 4
                requested_partitions.append(partition_id)
            
            requested_topics.append((topic_name, requested_partitions))
        
        print(f"📍 OffsetFetch for group: {group_id}, topics: {len(requested_topics)}", file=sys.stderr)
        
        # Build response
        header = struct.pack('>i', correlation_id)
        throttle_time_ms = struct.pack('>i', 0)
        
        # Topics array
        topics_array_length = encode_unsigned_leb128(len(requested_topics))  # UVarInt
        
        topics_response = b''
        for topic_name, partitions in requested_topics:
            # Topic name (compact string)
            topic_name_bytes = topic_name.encode('utf-8')
            topics_response += encode_unsigned_leb128(len(topic_name_bytes)) + topic_name_bytes
            
            # Partitions array
            partitions_array_length = encode_unsigned_leb128(len(partitions))  # UVarInt
            
            partitions_response = b''
            for partition_id in partitions:
                # Get stored offset or use -1 if not found
                stored_offset = self.consumer_offsets.get(group_id, {}).get(topic_name, {}).get(partition_id, -1)
                metadata = b''  # Empty metadata
                
                partition_response = (
                    struct.pack('>i', partition_id) +
                    struct.pack('>q', stored_offset) +
                    encode_unsigned_leb128(len(metadata)) + metadata +  # metadata as compact string
                    struct.pack('>h', 0)  # error_code = NO_ERROR
                )
                partitions_response += partition_response
                print(f"📍 Returning offset for {group_id}/{topic_name}[{partition_id}]: {stored_offset}", file=sys.stderr)
            
            topics_response += partitions_array_length + partitions_response
        
        final_tagged_fields = b'\x00'
        
        body = throttle_time_ms + topics_array_length + topics_response + final_tagged_fields
        response_data = header + body
        message_length = len(response_data)
        
        return struct.pack('>i', message_length) + response_data
    
    def create_error_response(self, correlation_id, error_code):
        """Create an error response"""
        header = struct.pack('>i', correlation_id)
        body = struct.pack('>h', error_code) + b'\x00' + struct.pack('>i', 0) + b'\x00'
        response_data = header + body
        message_length = len(response_data)
        return struct.pack('>i', message_length) + response_data

def main():
    server = KafkaServer()
    # Register save function to run on program exit
    atexit.register(server.save_all)
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n🛑 Server shutting down gracefully...", file=sys.stderr)
        server.save_all()
    except Exception as e:
        print(f"💥 Server error: {e}", file=sys.stderr)
        server.save_all()

if __name__ == "__main__":
    main()
