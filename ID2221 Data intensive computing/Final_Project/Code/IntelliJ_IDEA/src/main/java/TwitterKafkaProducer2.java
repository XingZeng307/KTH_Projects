import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterKafkaProducer2 {

//    private TwitterKafkaProducer() {
//        // when send message to kafka server, we need some properties through which we can connect to the kafka server
//        // the first property is bootstrap server, tell the location of Kafka Server
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "localhost:9092");
//        // serialization
//        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        //when send data to kafka server, need to send it as producer record object
//        //ProducerRecord("topic_name")
//
//        ProducerRecord producerRecord = new ProducerRecord("channel", "name", "selftuts");
//        //create instance of kafka producer and initialize the kafka producer with the above properties
//        KafkaProducer kafkaProducer = new KafkaProducer(properties);
//        kafkaProducer.send(producerRecord);
//    }
    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;
    //private Callback callback;
    public static final String CONSUMER_KEY = "CONSUMER_KEY";
    public static final String CONSUMER_SECRET = "CONSUMER_SECRET";
    public static final String ACCESS_TOKEN = "ACCESS_TOKEN";
    public static final String TOKEN_SECRET = "TOKEN_SECRET";
    public static final String HASHTAG = "#Trump";
        //define constant to configure Kafka producer
    public TwitterKafkaProducer2() {
        Authentication authentication = new OAuth1(
                CONSUMER_KEY,
                CONSUMER_SECRET,
                ACCESS_TOKEN,
                TOKEN_SECRET);
        // track the terms
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Collections.singletonList(HASHTAG));

        queue = new LinkedBlockingQueue<String>(10000);
        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();
        gson = new Gson();
        //callback = new BasicCallback();
    }
    private Producer<String, String> getProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        // serialization
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(properties);
//            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.SERVERS);
//            properties.put(ProducerConfig.ACKS_CONFIG, "1");
//            properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
//            properties.put(ProducerConfig.RETRIES_CONFIG, 0);
//            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
//            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }
    public void run() throws InterruptedException {
        client.connect();
        Producer<String, String> producer = getProducer();
        int i =0;
        while (i<100000) {
            Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
            System.out.printf("Fetched tweet id %d\n", tweet.getId());
            long key = tweet.getId();
            String key_string = String.valueOf(key) ;
            String msg = tweet.toString();
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("Itjob-tweets",key_string , msg);
            producer.send(record);
            i++;
        }
        client.stop();
    }
}

//add dependencies;
//<dependency>
//<groupId>org.slf4j</groupId>
//<artifactId>slf4j-simple</artifactId>
//<version>1.7.25</version>
//</dependency>

//setting main;
//add configuration;
//add application;
//main class Runner;


// create kafka topic
//  ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic channel --from-begining
