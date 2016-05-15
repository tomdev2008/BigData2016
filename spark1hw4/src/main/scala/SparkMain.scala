import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, Instant}
import java.util.Date

import org.apache.spark.SparkContext._

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by Vitaliy on 5/14/2016.
 */
object SparkMain {

  class LogItem() extends Serializable {
    var id: String = _
    //    var keywords: String = _
    var date: LocalDate = _
    var city: String = _

    //    def getId(): String = {
    //      return "id=" + id
    //    }

    override def toString = s"LogItem($id, $date, $city)"
  }


  case class DateCityKey(date: LocalDate, city: String) extends Serializable


  def main(args: Array[String]) {
    val key = new DateCityKey(LocalDate.now(), "")


    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf);
    val streamRdd = sc.textFile("D:\\projects\\BDCC\\BigData2016\\spark1hw2\\stream.txt")
      .map((line: String) => toLogItem(line))
      .map((item: LogItem) => (item.id, item))

    streamRdd.foreach((tuple: (String, LogItem)) => println(s"id=${tuple._1} item=${tuple._2}"))

    val keywordsRdd = sc.textFile("D:\\projects\\BDCC\\BigData2016\\spark1hw2\\keywords.txt")
      .map(toIdKeywordsPair)

    keywordsRdd.foreach((tuple: (String, Array[String])) => println(s"id=${tuple._1} tags=${tuple._2.mkString(",")}"))

    val tagsPerDateAndCity = streamRdd.join(keywordsRdd)
      .map(m)
      .aggregateByKey(Set[String]())((strings: Set[String], strings0: Array[String]) => strings ++ strings0, (strings: Set[String], strings0: Set[String]) => strings ++ strings0)
      .persist()

    tagsPerDateAndCity.foreach((tuple) => {
      println(s"dateCity=${tuple._1} tags=${tuple._2}")
    })

//    tagsPerDateAndCity.map()
  }

  def m(tuple: (String, (LogItem, Array[String]))): (DateCityKey, Array[String]) = {
    val logItem = tuple._2._1
    (new DateCityKey(logItem.date, logItem.city), tuple._2._2)
  }

  def toLogItem(line: String): LogItem = {
    val logItem = new LogItem()
    val items = line.split("\t")
    logItem.id = items(20)
    logItem.date = LocalDate.parse(items(1), DateTimeFormatter.ofPattern("uuuuMMddHHmmssnnn"))
    logItem.city = items(6)
    logItem
  }

  def toIdKeywordsPair(line: String): (String, Array[String]) = {
    val items = line.split("\t")
    (items(0), items(1).split(" "))
  }

  def toDate(date: String): Date = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.parse(date)
  }


}



