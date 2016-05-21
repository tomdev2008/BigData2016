import java.io.Serializable
import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util
import java.util.Date

import com.restfb.json.JsonObject

import scala.collection.JavaConversions._
import com.restfb.types.Event
import com.restfb.{Connection, DefaultFacebookClient, Parameter, Version}
import com.sun.xml.internal.fastinfoset.algorithm.BuiltInEncodingAlgorithm.WordListener
import org.apache.spark.sql.functions.{array, collect_list, lit, udf}
import org.apache.spark.sql.{DataFrameReader, Row, SQLContext, functions}
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.GenTraversableOnce
import scala.collection.mutable.ListBuffer

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

    val key = "EAACEdEose0cBALPsBncWTIU8bTMziIsxPZA0rdZAnHQwnP1nIIUZApNIIHrhO7lMbdJy6gGZBsEwRgRlyDp4JOROBLFuycDzqTUeK19i5i6kvElZBKA1jAHa6SfdSthR9AQc32yZCOz7ZADsx1iu7TIyFuVJiLEJVVjXZAFAMiZCIkwZDZD"

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

/*    val result = sqlContext.sql(
      "select s.timestamp, c.City, t.tags, c.Latitude, c.Longitude from stream s " +
        "join tags t on s.userTags = t.tagId " +
        "join city c on s.city = c.Id " //+
//        "group by f(s.timestamp), c.City"
    )

//    result.foreach(println)

    case class DateCityKey (localDate: Date, city: String, latitude: Double, longitude: Double) extends Serializable

    case class Value (tags: Array[String]) extends Serializable

    val keyValueRDD = result.map((row: Row) => {
      val date = new SimpleDateFormat("yyyyMMdd").parse(row.getString(0).substring(0, 8))
//      val date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("uuuuMMddHHmmssnnn"))
      val key: DateCityKey = DateCityKey(date, row.getString(1), row.getDouble(3), row.getDouble(4))
      (key, row.getString(2).split(" "))
    })

//    keyValueRDD.foreach(println)

    val grouperRdd = keyValueRDD.groupByKey()
      .mapValues((strings: Iterable[Array[String]]) => {
        strings.flatMap((strings: Array[String]) => strings).toSet
      })

//    grouperRdd.foreach(tuple => {
//      println(tuple)
//    })

    case class EventData(eventId: String, description: String, attendingCount: Integer) extends Serializable

    case class DateCityTagKey(dateCityKey: DateCityKey, tag: String) extends Serializable

    val withEventsRdd = grouperRdd.flatMap((tuple: (DateCityKey, Set[String])) => {
      val result = ListBuffer[(DateCityTagKey, List[EventData])]()

            tuple._2.foreach((tag: String) => {
              val fc = new DefaultFacebookClient(key, Version.LATEST)

              val connection: Connection[Event] = fc
                .fetchConnection(
                  "search",
                  classOf[Event],
                  Parameter.`with`("q", tag),
                  Parameter.`with`("type", "event"),
                  Parameter.`with`("limit", "200"),
                  Parameter.`with`("center", s"${tuple._1.latitude}%2C${tuple._1.longitude}"),
                  Parameter.`with`("distance", "1000"),
                  Parameter.`with`("fields", "id,description,attending_count")
                )

              val events = ListBuffer[EventData]()

              val it = connection.iterator()
              while(it.hasNext) {
                val batch: util.List[Event] = it.next()
                batch.toList.foreach((event: Event) => {
//                  val start = LocalDate.from(Instant.ofEpochMilli(event.getStartTime().getTime))
//                  val end = LocalDate.from(Instant.ofEpochMilli(event.getEndTime().getTime))
//                  if(event.getStartTime().before(tuple._1.localDate) && event.getEndTime() == null || event.getStartTime().before(tuple._1.localDate) && event.getEndTime() != null && event.getEndTime().after(tuple._1.localDate)) {
                  events += EventData(event.getId, event.getDescription, event.getAttendingCount)
//                  }
                })
              }

              val i = (DateCityTagKey(tuple._1, tag), events.toList)
              result += i



            })
      result
    })

//    withEventsRdd.foreach((tuple: (DateCityTagKey, List[EventData])) => {
//      println(tuple)
//    })



    val tokenized = withEventsRdd.mapValues((events: List[EventData]) => {
      val tokens = ListBuffer[String]()
      var attendee = 0
      events.foreach((event: EventData) => {
        val p = "\\b[^\\d\\W]+\\b".r
        if(event.description != null) {
          tokens appendAll p.findAllIn(event.description).toList
        }
        if(event.attendingCount != 0) {
          attendee += event.attendingCount
        }
      })
      println(s"ts=${tokens.size}")
//      val map = tokens.groupBy((s: String) => s).mapValues(_.size)



      val map = tokens.groupBy(word => word)
        .mapValues(_.size)
        .toSeq
        .sortBy(tuple => tuple._2)
        .reverse
        .take(10)


//      tokens.toList.map((word: String) => (word, 1))
//          .reduce((value: (String, Int), value0: (String, Int)) => (value._1, value._2 + value0._2))
      (attendee, map)
    })

//    tokenized.foreach((tuple: (DateCityTagKey, (Int, Map[String, Int]))) => {
//      println("!")
//    })

    val strings = tokenized
      .map((tuple: (DateCityTagKey, (Int, Seq[(String, Int)]))) => {
        val tokens = tuple._2._2.map((tuple: (String, Int)) => s"${tuple._1},${tuple._2}").mkString(",")
        s"${tuple._1.tag}\t${tuple._1.dateCityKey.localDate}\t${tuple._1.dateCityKey.city}\t${tuple._2._1}\t${tokens}"
      })

    strings.foreach(println)

//    println(tokenized.count())

    val eventIds = withEventsRdd.flatMapValues((datas: List[EventData]) => {
      datas.map((data: EventData) => data.eventId)
    }).values


    eventIds.foreach(println)


    def load(eventId: String) : Stream[AnyRef] = {
      try {
        val fc = new DefaultFacebookClient(key, Version.LATEST)

        val connection: Connection[JsonObject] = fc.fetchConnection(eventId + "/attending", classOf[JsonObject])

        connection.iterator().toStream.flatMap((objects: util.List[JsonObject]) => {
          objects.toList.map(_.get("name"))
        })
      } catch {
        case e: Exception => {
          println(s"error! wait! $eventId")
          e.printStackTrace()
          Thread.sleep(3000)
          println(s"retry! $eventId")
          load(eventId)
        }
      }
    }

    val attendeeNames = eventIds.flatMap((eventId: String) => {
      println(s"Attendee call $eventId")
      load(eventId)
    })*/


    val an = sc.parallelize(List("qwe asd", "qwe asd", "zxc zxc", "dfg aasd"))

    val sortedAttendee = an //attendeeNames
      .map(name => (name, 1))
      .reduceByKey(_+_)
      .sortBy((tuple: (AnyRef, Int)) => tuple._2, true)

    sortedAttendee.map((tuple: (String, Int)) => tuple._1)
      .foreach(println)

    println("e")

  }



}




