from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:19092,localhost:29092,localhost:39092',  # Replace with your Kafka broker addresses
    'client.id': 'python-producer'
}

# Create a producer instance
producer = Producer(conf)

# Produce several messages to the 'test-topic'
messages = ["Hello Kafka", "This is message 2", "Message number 3", "Last message"]
topic = 'test-topic'



# Callback function to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Produce messages
for message in messages:
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered
producer.flush()
