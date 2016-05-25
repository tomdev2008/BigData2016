package hw2

import com.datastax.spark.connector.toRDDFunctions
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
 * Created by Vitaliy on 5/22/2016.
 */
object SparkStreamingElasticsearchMain {

  // bin\windows\zookeeper-server-start.bat config\zookeeper.properties
  // bin\windows\kafka-server-start config\server.properties
  // bin\windows\kafka-console-producer --broker-list localhost:9092 --topic test
  // bin\windows\kafka-console-consumer --zookeeper localhost:2181 --topic test --from-beginning

  def main(args: Array[String]) {

    println("Start ")

    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

/*
    val topicsSet = Set("test")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)


    messages.map((tuple: (String, String)) => tuple._2)
        .print()
*/

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    sc.makeRDD(Seq(numbers, airports)).saveToCassandra()//saveToEs("spark/docs")

//    val lines = ssc.socketTextStream("10.23.11.64", 9999)
//      lines.map((s: String) => s + " !!!")
//      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
