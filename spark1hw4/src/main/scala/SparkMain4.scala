import java.io.Serializable
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.restfb.types.Event
import com.restfb.{Connection, DefaultFacebookClient, Parameter, Version}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrameReader, Row, SQLContext}

import scala.collection.JavaConversions._
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Created by Vitaliy on 5/14/2016.
 */
object SparkMain4 {

  def main(args: Array[String]) {
    println("Start")


    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

//    sc.



    //this rdd could be loaded from external source
    val rdd = sc.parallelize(List(
      ("q", List("a1", "b2", "c3", "b2", "c3", "c3")),
      ("w", List("a1", "b2", "c3", "b2", "c3", "c3"))
    ))

    rdd.saveAsTextFile("file:///D:/ttt")

    val c = rdd.flatMapValues((strings: List[String]) => strings)
    .map((tuple: (String, String)) => (tuple, 1))
    .reduceByKey(_+_)
    .map((tuple: ((String, String), Int)) => (tuple._1._1, (tuple._1._2, tuple._2)))
      .sortBy((tuple: (String, (String, Int))) => tuple._2._2, false)
      .groupByKey()
      .mapValues((tuples: Iterable[(String, Int)]) => tuples.take(2))
    .collect()

    print()
//    rdd.mapValues((strings: List[String]) => {
////      sc.parallelize(strings)
////        .map(word => (word, 1))
////        .reduceByKey(_+_)
////        .collect()
//      strings.groupBy(word => word)
//        .mapValues(_.size)
//        .toSeq
//        .sortBy(tuple => tuple._2)
//        .reverse
//        .take(2)
//        .toMap
//    }).foreach(println)



  }



}




