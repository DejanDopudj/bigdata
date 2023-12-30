from confluent_kafka import Consumer, KafkaError

def consume_from_kafka(bootstrap_servers, group_id, topic):
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'  
    }

    consumer = Consumer(consumer_conf)

    consumer.subscribe([topic])

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(timeout=1000) 

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print('Error: {}'.format(msg.error()))
                    break

            print('Received message: {}'.format(msg.value().decode('utf-8')))

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == '__main__':
    bootstrap_servers = '127.0.0.1:29092' 
    consumer_group_id = 'my-consumer-group'  
    kafka_topic = 'topic'  

    consume_from_kafka(bootstrap_servers, consumer_group_id, kafka_topic)
