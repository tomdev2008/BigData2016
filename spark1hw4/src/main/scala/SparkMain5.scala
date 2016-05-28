import java.io.Serializable
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.Date

import com.restfb.exception.FacebookOAuthException
import com.restfb.json.JsonObject
import com.restfb.types.{Place, Event}
import com.restfb.{Connection, DefaultFacebookClient, Parameter, Version}
import org.apache.hadoop.mapreduce.jobhistory.Events
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrameReader, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DatasetHolder._

import org.apache.spark.sql._

//import org.apache.spark.sql.sqlContext.implicits._

/**
 * Created by Vitaliy on 5/14/2016.
 */
object SparkMain5 {

  var key: String = _
  var input: String = _
  var output: String = _

  def main(args: Array[String]) {
    println("Start")
    if(args.length != 3) throw new IllegalArgumentException("Should be three parameters: face book key, input and output derectories")

    key = args(0)
    input = args(1)
    output = args(2)

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


    sc.parallelize(Seq(("", ""))).toDS

    // schema:
    // - Id
    // - City
    // - State Id
    // - Population
    // - Area
    // - Density
    // - Latitude
    // - Longitude
    val city = createCommonDFReader(sqlContext, true).load(input + "/city.us.txt")
    city.registerTempTable("city")
    city.printSchema()

    val keywordsSchema = StructType(Array(
      StructField("tagId", StringType, true),
      StructField("tags", StringType, true),
      StructField("f1", StringType, true),
      StructField("f2", StringType, true),
      StructField("f3", StringType, true),
      StructField("url", StringType, true)))

    val tags = createCommonDFReader(sqlContext, false)
      .schema(keywordsSchema)
      .load(input + "/tags.txt")
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

    val stream = createCommonDFReader(sqlContext, false)
      .schema(streamSchema)
      .load(input + "/stream.txt")
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

    val grouperEventsRdd = groupedDisclosedRdd.map((tuple: (DateCityKey, String)) => {
      val events = loadEventsByTagAndCity(tuple._2, tuple._1.latitude, tuple._1.longitude)
      (DateCityTagKey(tuple._1, tuple._2), events)
    })

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
      // make one word in one row
      .flatMapValues((descriptions: List[String]) => descriptions)
      //start word counting
      .map((tuple: (DateCityTagAmountKey, String)) => (tuple, 1))
      //end word counting
      .reduceByKey(_+_)
      // prepare tokens map as value
      .map((tuple: ((DateCityTagAmountKey, String), Int)) => (tuple._1._1, (tuple._1._2, tuple._2)))
      // make tokens map
      .sortBy((tuple: (DateCityTagAmountKey, (String, Int))) => tuple._2._2, false)
      .groupByKey()
      .mapValues((tuples: Iterable[(String, Int)]) => tuples.take(10))

    groupedTokenMapRdd
    .map((tuple: (DateCityTagAmountKey, Iterable[(String, Int)])) => {
      val tokens = tuple._2.map((tuple: (String, Int)) => s"${tuple._1},${tuple._2}").mkString(",")
      s"${tuple._1.dateCityTagKey.tag}\t${tuple._1.dateCityTagKey.dateCityKey.localDate}\t${tuple._1.dateCityTagKey.dateCityKey.city}\t${tuple._1.amount}\t${tokens}"
    })
    .saveAsTextFile(output + "/words")

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

    sortedAttendee.saveAsTextFile(output + "/names")

    println("end")
  }

  case class DateCityKey (localDate: LocalDate, city: String, latitude: Double, longitude: Double) extends Serializable

  case class EventData(eventId: String, description: String, attendingCount: Integer) extends Serializable

  case class DateCityTagKey(dateCityKey: DateCityKey, tag: String) extends Serializable

  case class DateCityTagAmountKey(dateCityTagKey: DateCityTagKey, amount: Int = 0) extends Serializable

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

      connection
        .toStream.flatMap((events: util.List[Event]) => events)
        .map((event: Event) => EventData(event.getId, event.getDescription, event.getAttendingCount))
        .toList
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

  def loadAttendee(eventId: String) : List[String] = {
    try {
      println(s"Loading Attendee by event $eventId ${Thread.currentThread().getName}")
      val fc = new DefaultFacebookClient(key, Version.LATEST)
      val connection: Connection[JsonObject] = fc.fetchConnection(eventId + "/attending", classOf[JsonObject])
      connection.iterator()
        .toStream
        .flatMap((objects: util.List[JsonObject]) => {
          objects.toList.map((place: JsonObject) => place.getString("name"))
        })
        .toList
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

}




