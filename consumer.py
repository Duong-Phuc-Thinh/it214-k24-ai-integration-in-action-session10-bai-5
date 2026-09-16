from kafka import KafkaConsumer
import json

def main():
    # Topic có 5 partitions. Để tránh lãng phí tài nguyên và đảm bảo hiệu quả tối đa,
    # số lượng Consumer trong một Consumer Group nên tối đa bằng 5.
    # Nếu bật > 5 consumer, các consumer thừa sẽ ở trạng thái rảnh (idle).
    
    consumer = KafkaConsumer(
        'order-tracking',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='order-tracking-group',
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print("Consumer started. Waiting for messages...")
    
    try:
        for message in consumer:
            event = message.value
            print(f"Received -> Partition: {message.partition}, Key: {message.key}, Event: {event}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()