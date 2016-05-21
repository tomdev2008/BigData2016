
import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate}
import java.util
import java.util.Date
import java.util.regex.{Matcher, Pattern}

import com.restfb.json.JsonObject

import scala.collection.JavaConversions._
import com.restfb.types.Event
import com.restfb.{Connection, DefaultFacebookClient, Parameter, Version}

/**
 * Created by Vitaliy on 5/15/2016.
 */
object Test {
  def main(args: Array[String]) {

    val map = Map(("as", "qw"),  ("12", "567"))
    println(map.map((tuple: (String, String)) => tuple._1 + "," + tuple._2).mkString(","))

//    Matcher matcher = Pattern.compile("\\b[^\\d\\W]+\\b").matcher(parsedText);
//    val matcher = Pattern.compile("\\b[^\\d\\W]+\\b").matcher("asd asf");
//    ma

//    val s = "asd w4t swregwg drsgb asd . 123"
//    val p = "\\b[^\\d\\W]+\\b".r
//    p.findAllIn(s).foreach(println)

//    LocalDate.from(Instant.ofEpochMilli(new Date().getTime()))
//
//    println("")

    //search?q=copyright&type=event&center=40.6643,-73.9385&distance=1000

//    val fc = new DefaultFacebookClient("EAACEdEose0cBAAaOZBfsoo5PLgONfZBYqP81NOFBdzEJdwiImExUnS2DhnJoQoC5hsG8PpcIcpsdtZBto8RA35nOxoTCmiuCkh4kgRUI70MqKmPezBUQih3uEsifHAiqZBz5urZAYaXgBvHoKJzc0wFm3Xr8vpvys3eDcKDrrZAAZDZD", Version.LATEST)
//
//    val connection: Connection[Event] = fc
//      .fetchConnection(
//        "search",
//        classOf[Event],
//        Parameter.`with`("q", "back"),
//        Parameter.`with`("type", "event"),
//        Parameter.`with`("limit", "200"),
//        Parameter.`with`("center", "40.6643%2C-73.9385"),
//        Parameter.`with`("distance", "1000")
//      )
//
//    // start first
//
//    val events: util.List[Event] = connection.getData
//
////    connection.iterator().toTraversable.foreach((events: util.List[Event]) => )
//
//    println(s"size in first = ${events.size()}")
//
//    events.toList
//      .foreach((e: Event) => {
////        println(e)
////        if(e.getAttendingCount != null) {
////          println("!!!")
////        }
//      })
//
//
//    // end first
//
//    val it = connection.iterator()
//    while(it.hasNext) {
//      val batch = it.next()
//      println(s"size=${batch.size()}")
//    }


        val fc = new DefaultFacebookClient("EAACEdEose0cBAHwmkh4twCefnvLtnDpg1DcpsqbUMZCX6DaDpZAbAKBKjQ9Plk9X9r7xoh1ZAyaWB3B2CCwyiJnmVdjHcFyVsuBdqoMzRF7SfUnQzEyFHizNmDksARqWpqZAakfly4knfEPJyYrn8ewaK2ZCGWuZAqiLBAMCeE0QZDZD", Version.LATEST)

        val connection: Connection[JsonObject] = fc
          .fetchConnection(
            "1085902651466512/attending",
            classOf[JsonObject]
          )

        // start first

        val attending: util.List[JsonObject] = connection.getData
        println(attending(0).get("name"))

    val names = connection.iterator().toStream.flatMap((objects: util.List[JsonObject]) => {
      objects.toList.map(_.get("name"))
    })

    println(names.mkString("|"))

  }

}
