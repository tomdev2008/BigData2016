package com.epam

//import kafka.serializer.StringDecoder
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.io.Serializable

import com.datastax.spark.connector.cql.CassandraConnectorConf.RetryDelayConf
import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql

/**
 * @author ${user.name}
 */
object TestApp {

  def main(args : Array[String]): Unit = {
    println("Start ")

    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)


    //save to ElasticSearch
//    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
//    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
//    val esConfig = Map("es.nodes" -> "localhost")
//    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs", esConfig)

//    case class LogItem(bidid: String, ipinyouid: String, timestamp: String) extends Serializable
//    val esConfig = Map("es.nodes" -> "localhost")
//    val rdd = sc.parallelize(List(LogItem("dd!!!", "sadasd", "asdasd")))
//      rdd.saveToEs("spark/docs", esConfig)


    //Save to Cassandra
    case class User(user_id: String, fname: String, lname: String) extends Serializable

    val collection = sc.parallelize(Seq(("101", "qq", "aa")))
    collection.saveToCassandra("hw2", "users")


    //Save to Cassandra

//    case class LogItem(bidid: String, ipinyouid: String, timestamp: String) extends Serializable
//
//        val collection = sc.parallelize(Seq(("q", "a", "z")))
////        val collection = sc.parallelize(List(()))
//        collection.saveToCassandra("hw2", "logs", SomeColumns("bidid", "ipinyouid", "timestamp"))



  }

}
