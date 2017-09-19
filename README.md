# Keras-Distributed-Streaming
Distributed Keras model for making predictions of the sentiment with Spanish sentences in stream context using Spark Streaming and Apache Kafka. The project has a script that allows you to enter phrases by command line and send them to a Kafka topic. Then Kafka sends the information to Spark Streaming where through the Jupyter Notebook shows the polarity of sentences on the screen.

## Prerequisites
**Files**:
- [Cardellino wor2vect file](http://crscardellino.me/SBWCE/)
**Software versions**:
- Spark 2.0.2 
- Apache Kafka
- Python 3.5
PENDIENTE version de kafka y especificaciones de compilar/sini compilar
**Library versions**:
- TensorFlow 1.2.1
- Keras 2.0.6
- Nltk 3.2.2

## Execution
First you have to start a ZooKeeper server
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Then you have to start a Kafka server
```
bin/kafka-server-start.sh config/server.properties
```
Next step is create a topic in Kafka named "test"
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```
Later, you have to open Jupyter-notebook writing the next line
```
$SPARK_HOME/bin/pyspark --master local[*] --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2
```
Finally you only have to follow the instruction of the notebook.

