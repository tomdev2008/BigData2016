package com.epam

//import kafka.serializer.StringDecoder
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.cql.CassandraConnectorConf.RetryDelayConf
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]): Unit = {
    println("Start ")

    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "10.23.11.64")
    val sc = new SparkContext(conf)


    //save to ElasticSearch
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    val esConfig = Map("es.nodes" -> "10.23.11.64")
    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs", esConfig)



    //Save to Cassandra
    val collection = sc.parallelize(Seq((10, "qq", "aa")))
    collection.saveToCassandra("mykeyspace", "users", SomeColumns("user_id", "fname", "lname"))



  }

}
