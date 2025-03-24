from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:19092,localhost:29092,localhost:39092',  # Replace with your Kafka broker addresses
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest available messages
}

# Create a consumer instance
consumer = Consumer(conf)

# Subscribe to the 'test-topic'
consumer.subscribe(['test-topic'])

# Consume messages
try:
    while True:
        msg = consumer.poll(1.0)  # Wait for a message with a timeout of 1 second

        if msg is None:
            # No message available within the timeout
            continue
        if msg.error():
            # Handle errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition
                print(f'End of partition reached: {msg.partition()}')
            else:
                raise KafkaException(msg.error())
        else:
            # Message successfully received
            print(f'Received message: {msg.value().decode("utf-8")}')
finally:
    # Close the consumer to clean up resources
    consumer.close()
