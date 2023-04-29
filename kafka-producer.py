import time

from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
import json

# create a producer. broker is running on localhost
producer = KafkaProducer(retries=5, bootstrap_servers=['localhost:9092'])


# define the on success and on error callback functions
def on_success(record):
    print(record.topic)
    print(record.partition)
    print(record.offset)


def on_error(excp):
    raise Exception(excp)


# send the message to fintechexplained-topic
while True:
    line = input()
    producer.send('spark-streaming-topic', json.dumps(line).encode("utf-8")).add_callback(on_success)\
        .add_errback(on_error)
    # block until all async messages are sent
    producer.flush()
    print("message sent.")
    time.sleep(1)
