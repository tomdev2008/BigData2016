import org.apache.spark.sql.types.{StructField, StringType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

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

    // schema:
    // - Id
    // - City
    // - State Id
    // - Population
    // - Area
    // - Density
    // - Latitude
    // - Longitude
    val city = createCommonDFReader(sqlContext, true)
      .load("D:\\projects\\BDCC\\BigData2016\\spark1hw4\\city.us.txt")
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
      .load("D:\\projects\\BDCC\\BigData2016\\spark1hw4\\tags.txt")
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
      .load("D:\\projects\\BDCC\\BigData2016\\spark1hw4\\stream.txt")
    stream.registerTempTable("stream")
    stream.printSchema()

    sqlContext.sql(
      "select s.userTags, s.timestamp, s.city from stream s"
    ).foreach(println)

    println("--------------")

    sqlContext.sql(
//      "select k.tags, k.tagId from keywords k"
      "select * from tags"
    ).foreach(println)

    println("--------------")

    sqlContext.sql(
      "select c.Id, c.City from city c"
    ).foreach(println)

    println("-------------->")

    sqlContext.sql(
      "select c.City, s.timestamp, t.tags from stream s " +
        "join tags t on s.userTags = t.tagId " +
        "join city c on s.city = c.Id " +
        ""
    ).foreach(println)

  }



}



