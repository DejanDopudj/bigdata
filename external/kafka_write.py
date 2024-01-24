from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_to_kafka(bootstrap_servers, topic, message):
    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'python-producer'
    }

    producer = Producer(producer_conf)

    try:
        producer.produce(topic, key=None, value=message, callback=delivery_report)

        producer.flush()

    finally:
        print("test")

# if __name__ == '__main__':
#     bootstrap_servers = '127.0.0.1:29092' 
#     kafka_topic = 'test' 

    # produce_to_kafka(bootstrap_servers, kafka_topic, messages_to_send)
