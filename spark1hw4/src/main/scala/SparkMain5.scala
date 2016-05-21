import java.io.Serializable
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.Date

import com.restfb.exception.FacebookOAuthException
import com.restfb.json.JsonObject
import com.restfb.types.Event
import com.restfb.{Connection, DefaultFacebookClient, Parameter, Version}
import org.apache.hadoop.mapreduce.jobhistory.Events
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrameReader, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Created by Vitaliy on 5/14/2016.
 */
object SparkMain5 {

  //variables
  val key = "EAACEdEose0cBAJ5JzZCYoW4fE3oeZB4xuDr1teU6dauZAoUIvwZCtHD4tVHYpIF1mfgYi2ZBsJ9eDJbnX2UKxgHScOVFC2esZC4ZC6ioNaDwddZCW09u3B2iF9C6LaPkITBrMTWAr4LN186MIENZCxbUKkLdLd3WJgIsoI812T2GpWwZDZD"

  case class DateCityKey (localDate: LocalDate, city: String, latitude: Double, longitude: Double) extends Serializable

  case class Value (tags: Array[String]) extends Serializable

  case class EventData(eventId: String, description: String, attendingCount: Integer) extends Serializable

  case class DateCityTagKey(dateCityKey: DateCityKey, tag: String) extends Serializable

  def createCommonDFReader(sqlContext: SQLContext, excludeHeader: Boolean): DataFrameReader = {
    val dataFrameReader = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
    if (excludeHeader) dataFrameReader.option("header", "true")
    dataFrameReader
  }

  def loadEventsByTagAndCity(tag: String, latitude: Double, longitude: Double): List[EventData] = {
    try {
      println(s"Loading Event tag=$tag latitude=$latitude longitude=$longitude")
      val connection: Connection[Event] = new DefaultFacebookClient(key, Version.LATEST)
        .fetchConnection(
          "search",
          classOf[Event],
          Parameter.`with`("q", tag),
          Parameter.`with`("type", "event"),
          Parameter.`with`("limit", "200"),
          Parameter.`with`("center", s"$latitude%2C$longitude"),
          Parameter.`with`("distance", "1000"),
          Parameter.`with`("fields", "id,description,attending_count")
        )

      //TODO refactor
      val events = ListBuffer[EventData]()

      val it = connection.iterator()
      while(it.hasNext) {
        val batch: util.List[Event] = it.next()
        batch.toList.foreach((event: Event) => {
          events += EventData(event.getId, event.getDescription, event.getAttendingCount)
        })
      }
      events.toList
    } catch {
      case e: FacebookOAuthException => {
        println(s"event loading error! wait! $tag")
        e.printStackTrace()
        Thread.sleep(5*60*1000)
        println(s"retry event! $tag")
        loadEventsByTagAndCity(tag, latitude, longitude)
      }
    }
  }

  def getAmountAndTokensTuple(event: EventData): (Int, List[String]) = {
    var tokens = List[String]()
    var attendee = 0
    val p = "\\b[^\\d\\W]+\\b".r
    if(event.description != null) {
      tokens = p.findAllIn(event.description).toList
    }
    if(event.attendingCount != null) {
      attendee += event.attendingCount
    }
    (attendee, tokens)
  }

  def loadAttendee(eventId: String) : Stream[AnyRef] = {
    try {
      println(s"Loading Attendee by event $eventId ${Thread.currentThread().getName}")
      val fc = new DefaultFacebookClient(key, Version.LATEST)
      val connection: Connection[JsonObject] = fc.fetchConnection(eventId + "/attending", classOf[JsonObject])
      connection.iterator().toStream.flatMap((objects: util.List[JsonObject]) => {
        objects.toList.map(_.get("name"))
      })
    } catch {
      case e: FacebookOAuthException => {
        println(s"error! wait! $eventId")
        e.printStackTrace()
        Thread.sleep(5*60*1000)
        println(s"retry! $eventId")
        loadAttendee(eventId)
      }
    }
  }

  def main(args: Array[String]) {
    println("Start")


    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

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

    val sqlResult = sqlContext.sql(
      "select s.timestamp, c.City, t.tags, c.Latitude, c.Longitude from stream s " +
        "join tags t on s.userTags = t.tagId " +
        "join city c on s.city = c.Id "
    )

    val keyValueRDD = sqlResult.map((row: Row) => {
      val date = LocalDate.parse(row.getString(0), DateTimeFormatter.ofPattern("uuuuMMddHHmmssnnn"))
      val dateCityKey: DateCityKey = DateCityKey(date, row.getString(1), row.getDouble(3), row.getDouble(4))
      val tags: Array[String] = row.getString(2).split(" ")
      (dateCityKey, tags)
    })

    val grouperRdd = keyValueRDD.groupByKey()
      .mapValues((strings: Iterable[Array[String]]) => {
        strings.flatMap((strings: Array[String]) => strings).toSet
      })

    val groupedDisclosedRdd = grouperRdd.flatMapValues((strings: Set[String]) => strings)

    case class DateCityTagAmountKey(dateCityTagKey: DateCityTagKey, amount: Int) extends Serializable

    val grouperEventsRdd = groupedDisclosedRdd.map((tuple: (DateCityKey, String)) => {
      val events = loadEventsByTagAndCity(tuple._2, tuple._1.latitude, tuple._1.longitude)
      (DateCityTagKey(tuple._1, tuple._2), events)
    })

    //TODO add union with places

    grouperEventsRdd.persist(StorageLevel.DISK_ONLY)

    //FIRST BRANCH

    val groupedTokenMapRdd = grouperEventsRdd
      .flatMapValues((events: List[EventData]) => events)
      // get event's attendee count and words from description
      .mapValues(getAmountAndTokensTuple)
      // sum attendee and words per key (date, city, tag)
      .reduceByKey((tuple: (Int, List[String]), tuple0: (Int, List[String])) => (tuple._1 + tuple0._1, tuple._2 ++ tuple0._2))
      // add total attendee amount to key
      .map((tuple: (DateCityTagKey, (Int, List[String]))) => (DateCityTagAmountKey(tuple._1, tuple._2._1), tuple._2._2))
//      // make one word in one row
      .flatMapValues((descriptions: List[String]) => descriptions)
//      //start word counting
      .map((tuple: (DateCityTagAmountKey, String)) => (tuple, 1))
//      //end word counting
      .reduceByKey(_+_)
//      // prepare tokens map as value
      .map((tuple: ((DateCityTagAmountKey, String), Int)) => (tuple._1._1, (tuple._1._2, tuple._2)))
//      // make tokens map
      .groupByKey()

    val r1 = groupedTokenMapRdd.collect() //TODO add saving to file and sort and limit

    //SECOND BRANCH
    val uniqueEventIdsRdd = grouperEventsRdd
      .map((tuple: (DateCityTagKey, List[EventData])) => tuple._2)
      .flatMap((datas: List[EventData]) => datas)
      .map((event: EventData) => event.eventId)
      .distinct()

    val attendeeRdd = uniqueEventIdsRdd.flatMap(loadAttendee)

    val sortedAttendee = attendeeRdd
      .map(name => (name, 1))
      .reduceByKey(_+_)
      .sortBy(tuple => tuple._2, false)
      .map(tuple => tuple._1)

    val r2 = sortedAttendee.collect()

    println("end")
  }



}




