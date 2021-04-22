import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TwitterKafkaProducer {
    //define constant to configure Kafka producer
    public TwitterKafkaProducer() {
        // when send message to kafka server, we need some properties through which we can connect to the kafka server
        // the first property is bootstrap server, tell the location of Kafka Server
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        // serialization
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // when send data to kafka server, need to send it as producer record object
        //ProducerRecord("topic_name")

        ProducerRecord producerRecord = new ProducerRecord("channel", "name", "selftuts");
        //create instance of kafka producer and initialize the kafka producer with the above properties
        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        kafkaProducer.send(producerRecord);
        kafkaProducer.close();
    }
}

//add dependencies;
//<dependency>
//    <groupId>org.slf4j</groupId>
//    <artifactId>slf4j-simple</artifactId>
//    <version>1.7.25</version>
//</dependency>

//setting main;
//add configuration;
//add application;
//main class Runner;


// create kafka topic
//  ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic channel --from-begining
//$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic It-tweets --from-beginning