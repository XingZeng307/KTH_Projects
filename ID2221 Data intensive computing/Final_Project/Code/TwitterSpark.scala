package sparkstreaming

import java.util.HashMap
import java.io._ 

import org.apache.spark.streaming.dstream.ConstantInputDStream
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
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.cassandra._
import com.datastax.driver.core.ResultSet
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

import scalaj.http._
//import akka.actor._
//import HttpMethods._

// import dispatch._
// import Defaults._

import org.json4s._
import org.json4s.jackson.JsonMethods._

object TwitterSpark {
  val file = new File("/home/melih/project/SCALA/result_job.txt")
  val bw = new BufferedWriter(new FileWriter(file))
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    //val cluster = Cluster.<FILL IN>
	val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS jobsearch_keyspace WITH REPLICATION ={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
	session.execute("CREATE TABLE IF NOT EXISTS jobsearch_keyspace.twitter_job_ad (t_id text PRIMARY KEY, t_url text);")
	session.execute("CREATE TABLE IF NOT EXISTS jobsearch_keyspace.job_recommend (job_url text PRIMARY KEY, user_id int, similarity_rate float);")


	val kafkaConf = Map(
	"metadata.broker.list" -> "localhost:9092",
	"zookeeper.connect" -> "localhost:2181",
	"group.id" -> "kafka-spark-streaming",
	"zookeeper.connection.timeout.ms" -> "1000")
	
	//	val sparkConf = new SparkConf().setAppName("Key_Avg")
	val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Itjob_tweets")	
	val ssc = new StreamingContext(sparkConf, Seconds(2))
	ssc.checkpoint(".")
	
	val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("Itjob-tweets"))



	var content = ""
	var job_url = ""
	var jobseerker_map = collection.mutable.HashMap.empty[Int,List[Int]]
	//case class result(Job_url: String, User_id: Int,Similarity_rate: Float)
	//var result_seq : scala.collection.mutable.Seq = Seq[(String,Int,Float)]()

	var listBuffer = new ListBuffer[String]()  //stores job_type skills
	var listBuffer2 = new ListBuffer[Integer]() //stores jobads skills byte-list
	//var result_seq = Seq[result]()
	
	var result_seq = Seq[(String,Int,Float)]()
	// var result_seq = Seq[(String,Int,Float)]()
	// var result_seq_collect = Seq[Seq[(String,Int,Float)]]()

	//val t_ID_URL = kafkaStream.map(data => (data._1, data._2.substring(data._2.indexOf("https"), data._2.indexOf("https")+23)))
	val t_ID_URL_first = kafkaStream.map(x => (x._1, x._2))
	//val t_ID_URL = t_ID_URL_first.filter(x => x._2.indexOf("https") > -1).map(x => (x._1, x._2.substring(x._2.indexOf("https"), x._2.indexOf("https")+23)))
	// key : link; value: id
	val t_ID_URL = t_ID_URL_first.filter(x => x._2.indexOf("https") != -1).map(x => (x._2.substring(x._2.indexOf("https"), x._2.indexOf("https")+23), x._1))
	// deal with each rdd to get details of each link

	// JOB_type_split_string
	// CassandraRow{software_engineer: Cassandra},
	// CassandraRow{software_engineer: Streaming},CassandraRow{software_engineer: Buck}
	// CassandraRow{software_engineer: SPARK}
	// CassandraRow{software_engineer: Shell Script}
	// CassandraRow{software_engineer: SQL Server}
	// CassandraRow{software_engineer: Design Pattern}
	// CassandraRow{software_engineer: Data Mining}
	// CassandraRow{software_engineer: IBM Mainframe}

	//t_ID_URL.saveToCassandra("jobsearch_keyspace", "twitter_job_ad", SomeColumns("t_id", "t_url"))

// [1,0,1,0] first get job_type_skills, then create jobAdsList 0/1 based on job_type_skills
    val cassandraRDD = ssc.cassandraTable("jobsearch_keyspace","job_type").select("software_engineer") 
	val dstream = new ConstantInputDStream(ssc, cassandraRDD)
	//for loop job_type_skills read from cassandra
	dstream.foreachRDD{ rdd => 
		var split_string = rdd.collect.mkString(",").split(",") //all skills of software_engineer

		for(each_string <- split_string){
			var key_word = each_string.substring(32,each_string.lastIndexOf("}"))
			//println("FETCH from cassandra about job_type_key_words3--------------------------------------"+key_word)
			listBuffer += key_word
			//println("insideBuffertoList--------------------------------"+listBuffer.toList)
			//println("insideBuffertoList_sss--------------------------------"+listBuffer.toList.distinct)


		}
	}

	//  Get JOB_SEEKERS skills from Cassandra (no real time)
	//CassandraRow{software_engineer: Cassandra},CassandraRow{software_engineer: Streaming},CassandraRow{software_engineer: Buck}
	// CassandraRow{software_engineer: SPARK}
	//CassandraRow{user_id: 26, skillset: [0,1,1,1,0,1,0,1,1,1,1,1,0,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,0,1,1,1,1,0,1,1,1,1,1,0,1,1,1,1,1,1,1,1,1]}
	val cassandraRDD_jobseerker = ssc.cassandraTable("jobsearch_keyspace","job_seeker").select("user_id","skillset").where("job_title=?", "software_engineer")
	val dstream_jobseerker = new ConstantInputDStream(ssc, cassandraRDD_jobseerker)
	// 	//  get job_seeker_skills from Cassandra
	// 	//skillset_list--------------------------------List(0, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1)
	dstream_jobseerker.foreachRDD{ rdd => 

		//println("insideBuffertoList--------------------------------"+listBuffer.toList)
		var split_string_jobseerker = rdd.collect.mkString("@").split("@")
		//println("split_string--------------------------------"+split_string)


		for(each_string <- split_string_jobseerker){
			var user_id = each_string.substring(22,each_string.indexOf(",")).toInt
			var skillset = each_string.substring(each_string.indexOf("skillset")+11,each_string.indexOf("}")-1)
			//println("FETCH from cassandra about job_seeker_user_id --------------------------------------"+user_id)
			//listBuffer += key_word
			//println("skillset--------------------------------"+skillset)

			var skillset_list: List[Int] = skillset.split(",").map(_.trim).toList.map(_.toInt)
			//println("skillset_list--------------------------------"+skillset_list)
			// println("similarity_list--------------------------------"+similarity_list)
			// println("similarity_list_sum--------------------------------"+similarity_list.sum)
			jobseerker_map += (user_id -> skillset_list)
		}
		
		//println("jobseerker_map--------------------------------"+jobseerker_map)
		//-Map(137 -> List(0, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1), 146 -> List(0,
	}



	t_ID_URL.foreachRDD{ rdd =>
		//rdd.keys.take(1).foreach(println)
		rdd.keys.take(10).foreach(JOB_AD_details)
		//println("MEL__key_is_.......................................................")
	}

		//println("insideBuffertoList--------------------------------"+listBuffer.toList.sorted)
	
	// first url as input, call  parse_header to get real link
	def JOB_AD_details(key_url: String)
	{
		println("key_test: " + key_url)
		// Http request
		val response: HttpResponse[String] = Http(key_url).asString

		
		// headers includes real link, use parse_header to parse this link // maplike
		val complete_URL = response.headers.values.take(10).foreach(parse_header)
		
		// Set(muc=48fcff36-58bc-48d0-aff8-231e6481ea05; Max-Age=63072000; 
		// 	Expires=Tue, 25 Oct 2022 00:47:09 GMT; Domain=t.co; Secure; 
		// 	SameSite=None, 121, Sun, 25 Oct 2020 00:52:09 GMT, Origin, 
		// 	https://twitter.com/JVER1/status/1320134706759933952/photo/1, private,max-age=300, tsa_o, Sun, 25 Oct 2020 00:47:09 GMT, 0, 
		// 	HTTP/1.1 301 Moved Permanently, max-age=0, fda4cf62e6db3764aeab52d24b1bda1c)

		// Set(https://www.thesun.co.uk/news/13013981/liberals-turn-ex-obama-adviser-cnn-van-jones-trump/?utm_source=knewz, muc=af62c97f-a6b2-4340-8b43-7ed4b74d4221; 
		// 	Max-Age=63072000; Expires=Tue, 25 Oct 2022 00:47:11 GMT; Domain=t.co; Secure; SameSite=None, Origin, 116, private,max-age=300, tsa_o, 0, 
		// 	HTTP/1.1 301 Moved Permanently, max-age=0, fda4cf62e6db3764aeab52d24b1bda1c, Sun, 25 Oct 2020 00:47:11 GMT, Sun, 25 Oct 2020 00:52:11 GMT)


		// HTTP_BODY_COMPLETE
		//val response_2: HttpResponse[String] = Http(complete_URL).asString
		// println("HTTP_BODY_COMPLETE: " + response_2.body)

	
		//HTTP_HEADERS: Map(cache-control -> Vector(private,max-age=300), content-length -> Vector(0), date -> Vector(Sat, 24 Oct 2020 15:28:20 GMT), expires -> Vector(Sat, 24 Oct 2020 15:33:20 GMT), location -> Vector(https://twitter.com/Laurie_Garrett/status/1319648516919205889), server -> Vector(tsa_o), set-cookie
		
	}


	//def parse_header(values: IndexedSeq[String] ): Unit = {}


	// get real link and read all info and compare and create [0,1,0,1] 
	def parse_header(values: IndexedSeq[String] ){
		var i: Int = 0
		for(value <- values)
		{	
			println("VALUE_TEST:" + i + "value===="+ value)
			if(value.contains("https")){
				println("VALUE_TEST_contains:" + i + "value===="+ value)
				// value: real link; content: HTTP_BODY
				job_url = value
				val response_2: HttpResponse[String] = Http(value).asString
				content = response_2.body
				//see if response_2.body contains keywords
				//var idx1: Int = 0
				for (ele <- listBuffer.toList.distinct.sorted){
					if(content.contains(ele)){
						listBuffer2 += 1
						//println(">>>>>>Matched skill:" + ele + "ïdx:" + idx1)
					}
					else{
						listBuffer2 += 0
					}
					//idx1 += 1
				}
				var jobAdsList = listBuffer2.toList		//get CV_list from map and caculate the similarity

				//FileWriter
			// 	//  get job_seeker_skills from Cassandra
			////-Map(137 -> List(0, 1, 1, 1, 0
			// 	//skillset_list--------------------------------List(0, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1)
				//for loop job_seeker
				for ((key,value) <- jobseerker_map){
					var skillset_list = value
					var person_count = skillset_list.filter(_>0).length
					var jobAds_count = jobAdsList.filter(_>0).length
					var similarity_list = skillset_list.zip(jobAdsList).map{case(x,y) => x*y }  
					println("common_Length    "+similarity_list.sum+" person   "+ person_count + "  job_size   " + jobAds_count)
					var similarity_rate = similarity_list.sum.toFloat / jobAds_count.toFloat
					//result_seq = result_seq :+ result(job_url,key,similarity_rate)
					
					//result_seq += Map("job_url"->job_url, "user_id"-> key, "similarity_rate"->similarity_rate)
					
					result_seq = Seq((job_url,key,similarity_rate))

					//result_seq += Seq(job_url,key,similarity_rate)
					//result_seq_collect = result_seq_collect :+ result_seq
					//List(result(https://twitter.com/BlessUSA45/status/1320294946423906304,137,1.0), result(https://twitter.com/BlessUSA45/status/1320294946423906304,146,1.0), result(https://twitter.com/BlessUSA45/status/1320294946423906304,92,1.0), result(https://twitter.com/BlessUSA45/status/1320294946423906304,101,1.0),
					//println("result_seq_inside3--------------------------------"+result_seq)
					bw.append(job_url+"	"+ key + "	" + similarity_list.sum+ "	"+ person_count + "	" +jobAds_count+ "	" + similarity_rate+"\n")

				}

			}
		}
	}



	

// 	// CassandraRow{software_engineer: Cassandra},CassandraRow{software_engineer: Streaming},CassandraRow{software_engineer: Buck}
// 	// CassandraRow{software_engineer: SPARK}
// 	// CassandraRow{software_engineer: Shell Script}
// 	// CassandraRow{software_engineer: SQL Server}
// 	// CassandraRow{software_engineer: Design Pattern}
// 	// CassandraRow{software_engineer: Data Mining}
// 	// CassandraRow{software_engineer: IBM Mainframe}

	//println("result_seq_save_seq5--------------------------------"+result_seq)


 	// t_ID_URL.saveToCassandra("jobsearch_keyspace", "twitter_job_ad", SomeColumns("t_id", "t_url"))
 	// val result_save = ssc.sparkContext.parallelize(result_seq)
 	// result_save.saveToCassandra("jobsearch_keyspace","job_recommend",SomeColumns("job_url","user_id","similarity_rate"))
    ssc.start()
    ssc.awaitTermination()
	
	//t_ID_URL.print()
  }
}
