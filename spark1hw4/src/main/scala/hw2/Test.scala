package hw2

import java.io.Serializable
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util

import com.restfb.exception.FacebookOAuthException
import com.restfb.json.JsonObject
import com.restfb.types.Event
import com.restfb.{Connection, DefaultFacebookClient, Parameter, Version}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

//import org.apache.spark.sql.sqlContext.implicits._

/**
  * Created by Vitaliy on 5/14/2016.
  */
object Test {

  var key: String = _
  var input: String = _
  var output: String = _

  def main(args: Array[String]) {
    println("Start")
    //    if(args.length != 3) throw new IllegalArgumentException("Should be three parameters: face book key, input and output derectories")

    //    key = args(0)
    //    input = args(1)
    //    output = args(2)

    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    case class Foobar(foo: String, bar: Integer) extends Serializable

//    val foobarRdd = sc.parallelize(Seq(Foobar("qwe", 23)))
    val foobarRdd = sc.parallelize(Seq("qwe"))

    sqlContext.createDataset(foobarRdd)

    //    val foobarDf = foobarRdd.toDF
    //    foobarDf.limit(1).show


    val ds = sc.parallelize(Seq(("", ""))).toDS


    case class Data(a: Int, b: String)
    val ds2 = sc.parallelize(Seq(
      Data(1, "one"),
      Data(2, "two"))).toDS

    ds2.collect()


    case class Stream(id: Int, typeId: Int) extends Serializable
    case class Type(id: Int, typeName: String) extends Serializable
    case class City(id: Int, city: String) extends Serializable

    val sRdd = sc.parallelize(Seq[Stream](Stream(10, 1), Stream(11, 2)))
    val streams: Dataset[Stream] = sRdd.toDS
    val types: Dataset[Type] = sc.parallelize(List(Type(1, "click"), Type(2, "pay"))).toDS
    val joined: Dataset[(Stream, Type)] = streams.joinWith(types, $"typeId" === $"id")

    sc.parallelize(Seq(("", ""))).toDS


    println("end")
  }

}




