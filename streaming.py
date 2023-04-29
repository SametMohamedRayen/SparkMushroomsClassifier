from pyspark import SparkContext
from pyspark.streaming import StreamingContext, DStream
from kafka import KafkaConsumer
import json


# Define the function to calculate the percentage of lines starting with "p" and "e"
def calculate_percentage(rdd):
    # Count the total number of lines
    total_lines = rdd.count()

    # Filter the lines starting with "p" and "e"
    p_lines = rdd.filter(lambda line: line.startswith('p')).count()
    e_lines = rdd.filter(lambda line: line.startswith('e')).count()

    # Calculate the percentage of lines starting with "p" and "e"
    p_percentage = (p_lines / total_lines) * 100 if total_lines > 0 else 0
    e_percentage = (e_lines / total_lines) * 100 if total_lines > 0 else 0

    # Print the results
    print(f'Percentage of lines starting with "p": {p_percentage}%')
    print(f'Percentage of lines starting with "e": {e_percentage}%')


# Create a Spark context with a batch interval of 5 seconds
sc = SparkContext("local[2]", "StreamingExample")
ssc = StreamingContext(sc, 1)

lines = ssc.textFileStream("stream_source.txt")
lines.foreachRDD(calculate_percentage)
ssc.start()

# To consume from fintechexplained-topic
consumer = KafkaConsumer('spark-streaming-topic', group_id='my-group', enable_auto_commit=False,
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
for message in consumer:
    # Process message
    print(f"Received message: {message.value}")
    with open('stream_source.txt', 'a') as the_file:
        the_file.write(message.value)

ssc.awaitTermination()

# lines = ssc.textFileStream("127.0.0.1:9092")

# Define the processing logic on the DStream
# lines.foreachRDD(calculate_percentage)

# Start the streaming context
# ssc.start()

# Wait for the streaming context to terminate
# ssc.awaitTermination()
