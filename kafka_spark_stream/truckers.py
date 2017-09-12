"""
To execute standalone: /usr/local/spark-2.0.2-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 truckers.py

See: https://www.rittmanmead.com/blog/2017/01/getting-started-with-spark-streaming-with-python-and-kafka/
"""
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json

#    Create Spark Context
sc = SparkContext(appName="TruckerStream_01")
sc.setLogLevel("WARN")

#    Create Streaming Context
ssc = StreamingContext(sc, 20)

#   Connect to Kafka
kafkaTruckersStream = KafkaUtils.createStream(ssc, 'sandbox.hortonworks.com:2181', 'truck-watchers', {'truckers':1})

#    Count trucker
truckers = kafkaTruckersStream.flatMap(lambda line: line.split(","))

pairs = truckers.map(lambda trucker: (trucker, 1))
truckerCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print each batch
truckerCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
