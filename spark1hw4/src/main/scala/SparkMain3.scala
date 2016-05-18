import java.io.Serializable
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date

import org.apache.spark.sql.functions.{array, collect_list, lit, udf}
import org.apache.spark.sql.{DataFrameReader, Row, SQLContext, functions}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.GenTraversableOnce

/**
 * Created by Vitaliy on 5/14/2016.
 */
object SparkMain3 {

  def createCommonDFReader(sqlContext: SQLContext, excludeHeader: Boolean): DataFrameReader = {
    val dataFrameReader = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
    if (excludeHeader) dataFrameReader.option("header", "true")
    dataFrameReader
  }

  def main(args: Array[String]) {
    println("Start")

    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val f = (s: String) => s.substring(0, 8)//TODO return date
    val myConcat = udf(f)
    sqlContext.udf.register("f", f)

    // schema:
    // - Id
    // - City
    // - State Id
    // - Population
    // - Area
    // - Density
    // - Latitude
    // - Longitude
    val cityPath = getClass.getResource("/city.us.txt").getPath()
    val city = createCommonDFReader(sqlContext, true).load(cityPath)
    city.registerTempTable("city")
    city.printSchema()

    val keywordsSchema = StructType(Array(
      StructField("tagId", StringType, true),
      StructField("tags", StringType, true),
      StructField("f1", StringType, true),
      StructField("f2", StringType, true),
      StructField("f3", StringType, true),
      StructField("url", StringType, true)))

    val tagsPath = getClass.getResource("/tags.txt").getPath()
    val tags = createCommonDFReader(sqlContext, false)
      .schema(keywordsSchema)
      .load(tagsPath)
    tags.registerTempTable("tags")
    tags.printSchema()

    val streamSchema = StructType(Array(
      StructField("bidId", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("iPinyouId", StringType, true),
      StructField("userAgent", StringType, true),
      StructField("ip", StringType, true),
      StructField("region", StringType, true),
      StructField("city", StringType, true),
      StructField("adExchange", StringType, true),
      StructField("domain", StringType, true),
      StructField("url", StringType, true),
      StructField("anonymous", StringType, true),
      StructField("adSlotId", StringType, true),
      StructField("adSlotWidth", StringType, true),
      StructField("adSlotHeight", StringType, true),
      StructField("adSlotVisibility", StringType, true),
      StructField("adSlotFormat", StringType, true),
      StructField("adSlotFloorPrice", StringType, true),
      StructField("creativeId", StringType, true),
      StructField("biddingPrice", StringType, true),
      StructField("advertiser", StringType, true),
      StructField("userTags", StringType, true),
      StructField("streamId", IntegerType, true)))

    val streamPath = getClass.getResource("/stream.txt").getPath()
    val stream = createCommonDFReader(sqlContext, false)
      .schema(streamSchema)
      .load(streamPath)
    stream.registerTempTable("stream")
    stream.printSchema()

    sqlContext.udf.register("sl", (s: String) => s.length)

//    sqlContext.udf.register("toDate", (timestamp: String) => LocalDate.parse(timestamp, DateTimeFormatter.ofPattern("uuuuMMddHHmmssnnn")))
//    sqlContext.udf.register("toDate", (timestamp: String) => {
//      val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
//      format.parse(timestamp)
//    })



//    sqlContext.sql(
//      "select f(s.timestamp), s.city from stream s"
//    ).foreach(println)

    println("--------------")

//    sqlContext.sql(
////      "select k.tags, k.tagId from keywords k"
//      "select * from tags"
//    ).foreach(println)
//
//    println("--------------")
//
//    sqlContext.sql(
//      "select c.Id, c.City from city c"
//    ).foreach(println)

    println("-------------->")

    val result = sqlContext.sql(
      "select s.timestamp, c.City, t.tags from stream s " +
        "join tags t on s.userTags = t.tagId " +
        "join city c on s.city = c.Id " //+
//        "group by f(s.timestamp), c.City"
    )

    result.foreach(println)

    case class DateCityKey (localDate: LocalDate, city: String) extends Serializable

    val keyValueRDD = result.map((row: Row) => {
      val date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("uuuuMMddHHmmssnnn"))
      (DateCityKey(date, row.getString(1)), row.getString(2).split(" "))
    })

    keyValueRDD.foreach(println)

    val grouperRdd = keyValueRDD.groupByKey()
      .mapValues((strings: Iterable[Array[String]]) => {
        strings.flatMap((strings: Array[String]) => strings).toSet
      })

    grouperRdd.foreach(tuple => {
      println(tuple)
    })

  }



}



