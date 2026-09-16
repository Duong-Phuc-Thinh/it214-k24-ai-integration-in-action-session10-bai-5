from kafka import KafkaProducer
import json
import time

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic} [partition {msg.partition}] with key {msg.key}")

def main():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic = 'order-tracking'
    order_id = 'ORDER-12345'

    events = [
        {"orderId": order_id, "status": "CREATED"},
        {"orderId": order_id, "status": "PAID"},
        {"orderId": order_id, "status": "SHIPPED"}
    ]

    for event in events:
        # Sử dụng order_id làm Partition Key để đảm bảo cùng đi vào 1 partition
        producer.send(
            topic,
            key=order_id,
            value=event
        )
        print(f"Sent event: {event['status']} for order: {order_id}")
        time.sleep(1)

    producer.flush()
    producer.close()

if __name__ == '__main__':
    main()