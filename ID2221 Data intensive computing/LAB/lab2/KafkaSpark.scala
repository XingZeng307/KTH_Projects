package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    //val cluster = Cluster.<FILL IN>
	val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION ={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
	session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")


	val kafkaConf = Map(
	"metadata.broker.list" -> "localhost:9092",
	"zookeeper.connect" -> "localhost:2181",
	"group.id" -> "kafka-spark-streaming",
	"zookeeper.connection.timeout.ms" -> "1000")
	

	val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Key_Avg")	
	val ssc = new StreamingContext(sparkConf, Seconds(2))
	ssc.checkpoint(".")
	
	
	val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("avg"))
	
	// sample value:
	// value="z,19"
	val pairs = kafkaStream.map(data => (data._2.split(",")(0),Integer.parseInt(data._2.split(",")(1))))
	

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Int], state: State[(Double, Int)]): (String, Double) = {
		val newVal = value.getOrElse(0)
		val (oldAvg, oldCount) = state.getOption.getOrElse((0.0, 0))
		val newCount = oldCount + 1
		val newAvg = (oldAvg * oldCount + newVal)/newCount
		state.update((newAvg, newCount))
		(key, newAvg)
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))
	print("stateDstream: " + stateDstream + "\n")

    // store the result in Cassandra
	stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
	
	stateDstream.print()
  }
}
