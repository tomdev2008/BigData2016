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

//    val map = List("q1", "q2", "q3", "q2", "q3", "q3")
//      .groupBy(word => word)
//      .mapValues(_.size)
//      .toSeq
//      .sortBy(tuple => tuple._2)
//      .reverse
//      .take(2)
//      .toMap
//
//    println(map)



    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val map = sc.parallelize(List((1, "asd"), (3, "asdasd"), (2, "sdsdsdjj")))
    val c = map.sortByKey(false)
      .collect()


    val an = sc.parallelize(List("qwe asd","zxc zxc", "qwe asd", "qwe asd", "zxc zxc", "dfg aasd"))

    val counted = an //attendeeNames
      .map(name => (name, 1))
      .reduceByKey(_+_)

    val c2 = counted.collect()

    val sortedAttendee = counted
      .map(item => item.swap)
      .sortByKey(false)

    val c3 = sortedAttendee.collect()
//      .map(item => item.swap)

    c3.foreach(println)

    val c4 = sortedAttendee.map(tuple => tuple._2)
    val c5 = c4.collect()

    println("---")

    println(c5(0))
    println(c5(1))
    println(c5(2))

    println("---")

    c4.saveAsTextFile("file:///D:\\test")

//    c4.foreach(println)

    println("end")


    //this rdd could be loaded from external source
//    val rdd = sc.parallelize(List(
//      ("q", List("q1", "q2", "q3", "q2", "q3", "q3")),
//      ("w", List("w1", "w2", "w3", "w2", "w3", "w3"))
//    ))
//
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




