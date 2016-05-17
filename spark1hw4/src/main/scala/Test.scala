
import java.text.SimpleDateFormat
import java.util

import scala.collection.JavaConversions._

import com.restfb.types.Event
import com.restfb.{Connection, Parameter, Version, DefaultFacebookClient}

/**
 * Created by Vitaliy on 5/15/2016.
 */
object Test {
  def main(args: Array[String]) {
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    println(format.parse("20130611232904865"))


//
//    val list = new util.ArrayList[String]()
//    list.add("1")
//    list.add("2")
////    list.forEach()
//
////    for (s <- list) println(s)
//
//    val fc = new DefaultFacebookClient("EAACEdEose0cBAGDVOCGl7pjxbCvo2wFV8nsrG2kCsZCfJx5aw4hJjPKbYQEI9MkjlVDS94QlYZBOML9YAp2RTe7CyDmPv3oogekPRaQdr43bi4DNaZC5nqo8XTk3dBKvgSsB3f3kFHm83X3WD9ySGzfnNTVK8aTQikEq1bRdAZDZD", Version.LATEST)
////    fc.fetchConnection("search", Event.class, Parameter.with("q", "Mark"), Parameter.with("type", "event"));
//    val connection: Connection[Event] = fc.fetchConnection("search", classOf[Event], Parameter.`with`("q", "copyright"), Parameter.`with`("type", "event"))
//    val events: util.List[Event] = connection.getData
////    events.forEach(_ => {println("")})
//    events.toList.foreach((s) => println(s))
//
////    for (event <- events) println(event)

  }

}
