#!/usr/bin/env python3
import json
import os
import time

def test_persistence_verification():
    print("🧪 Persistence Verification Test")
    print("=" * 40)
    
    store_file = "kafka_persistent_store.json"
    
    if not os.path.exists(store_file):
        print("❌ Persistence file does not exist")
        return False
    
    try:
        with open(store_file, 'r') as f:
            store = json.load(f)
        
        print("✅ Persistence file exists")
        print(f"📊 Store contents: {json.dumps(store, indent=2)}")
        
        # Check structure
        if 'test-topic' in store:
            topic_data = store['test-topic']
            if '0' in topic_data:
                messages = topic_data['0']
                print(f"📝 Found {len(messages)} messages in test-topic[0]")
                if messages:
                    print(f"💬 Latest message: {messages[-1]}")
                    return True
                else:
                    print("⚠️ No messages in topic")
                    return False
            else:
                print("❌ No partition 0 in test-topic")
                return False
        else:
            print("❌ No test-topic in store")
            return False
            
    except Exception as e:
        print(f"❌ Error reading persistence file: {e}")
        return False

if __name__ == "__main__":
    success = test_persistence_verification()
    if success:
        print("\n🎉 Persistence verification PASSED!")
    else:
        print("\n💥 Persistence verification FAILED!")
