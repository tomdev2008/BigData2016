package com.epam

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date

import com.datastax.spark.connector.streaming.toDStreamFunctions
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import eu.bitwalker.useragentutils.{Browser, OperatingSystem, UserAgent}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.SparkRDDFunctions
import org.apache.spark._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * @author ${user.name}
 */
object SparkHw2 {


/*

    *** Start text environment

    bin\windows\zookeeper-server-start.bat config\zookeeper.properties
    bin\windows\kafka-server-start config\server.properties
    bin\windows\kafka-console-producer --broker-list localhost:9092 --topic test
    bin\windows\kafka-console-consumer --zookeeper localhost:2181 --topic test --from-beginning

    bin\elasticsearch
    bin\kibana

    bin\cassandra




    *** Preparing Cassandra

    CREATE KEYSPACE hw2 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

    CREATE TABLE stream (
      bid_id text PRIMARY KEY,
      timestamp text,
      i_pinyou_id text,
      user_agent text,
      ip text,
      region text,
      ad_exchange text,
      domain text,
      url text,
      anonymous text,
      ad_slot_id text,
      ad_slot_width text,
      ad_slot_height text,
      ad_slot_visibility text,
      ad_slot_format text,
      ad_slot_floor_price text,
      creative_id text,
      bidding_price text,
      advertiser text,
      user_tags text,
      stream_id int,
      stream_type text,
      tags text,
      city text,
      state_id int,
      population int,
      area float,
      density float,
      latitude float,
      longitude float
    );




    *** Preparing Elasticksearch

    {
     "mappings" : {
      "_default_" : {
       "properties" : {
        "iPinyouId" : {"type": "string", "index" : "not_analyzed" },
        "timestamp" : {"type": "date", "index" : "not_analyzed" },
        "biddingPrice" : {"type": "integer", "index" : "not_analyzed" },
        "streamType" : {"type": "string", "index" : "not_analyzed" },
        "uaBrowserType" : {"type": "string", "index" : "not_analyzed" },
        "uaBrowserName" : {"type": "string", "index" : "not_analyzed" },
        "uaOsNmae" : {"type": "string", "index" : "not_analyzed" },
        "uaDevice" : {"type": "string", "index" : "not_analyzed" },
        "city" : {"type": "string", "index" : "not_analyzed" },
        "coordinate" : {"type": "geo_point", "index" : "not_analyzed" }
       }
      }
     }
    }


   */

  var cityPath: String = _
  var logTypesPath: String = _
  var tagsPath: String = _
  var esOut: String = _
  var cassandraKeyspace: String = _
  var cassandraTable: String = _


  def main(args : Array[String]): Unit = {
    println("Start")
    if(args.length != 6) throw new IllegalArgumentException("Illegal number of parameters")

    cityPath = args(0)
    logTypesPath = args(1)
    tagsPath = args(2)
    esOut = args(3)
    cassandraKeyspace = args(4)
    cassandraTable = args(5)


    // Configuration
    val conf = new SparkConf()
      .setAppName("HW1")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    val topicsSet = Set("test")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")

    // Creating dictionary RDDs
    val city: RDD[(Int, City)] = sc.textFile(cityPath)
      .map((line: String) => line.split("\t"))
      .map((items: Array[String]) => City(items(0).toInt, items(1), items(2).toInt, items(3).toInt,
          items(4).toFloat, items(5).toFloat, items(6).toFloat, items(7).toFloat))
      .map((city: City) => (city.id, city))
      .cache()

    val logType: RDD[(Int, LogType)] = sc.textFile(logTypesPath)
      .map((line: String) => line.split("\t"))
      .map((items: Array[String]) => LogType(items(0).toInt, items(1)))
      .map((logType: LogType) => (logType.id, logType))
      .cache()

    val tags: RDD[(String, Tags)] = sc.textFile(tagsPath)
      .map((line: String) => line.split("\t"))
      .map((items: Array[String]) => Tags(items(0), items(1)))
      .map((tags: Tags) => (tags.id, tags))
      .cache()


    // Retrieve data from Kafka
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

//    stream.join()

    val records: DStream[StreamRecord] = stream.map((tuple: (String, String)) => tuple._2)
      .map((line: String) => line.split("\t"))
      .map((items: Array[String]) => {
        val date = new SimpleDateFormat("yyyyMMddHHmmssSSS").parse(items(1))
        StreamRecord(items(0), date, items(2), items(3), items(4), items(5), items(6).toInt,
          items(7), items(8), items(9), items(10), items(11), items(12), items(13), items(14),
          items(15), items(16), items(17), items(18), items(19), items(20), items(21).toInt)
      })
      .filter((record: StreamRecord) => record.iPinyouId != "null")
      .filter((record: StreamRecord) => record.streamId != 0)

    val fullRecords: DStream[FullStreamRecord] = records.transform((rdd: RDD[StreamRecord]) => {
      rdd.map((record: StreamRecord) => (record.city, record))
        .join(city)
        .map((tuple: (Int, (StreamRecord, City))) => FullStreamRecord(tuple._2._1, tuple._2._2, null, null))
        .map((full: FullStreamRecord) => (full.record.streamId, full))
        .join(logType)
        .map((tuple: (Int, (FullStreamRecord, LogType))) => FullStreamRecord(tuple._2._1.record, tuple._2._1.city, tuple._2._2, null))
        .map((full: FullStreamRecord) => (full.record.userTags, full))
        .join(tags)
        .map((tuple: (String, (FullStreamRecord, Tags))) => FullStreamRecord(tuple._2._1.record, tuple._2._1.city, tuple._2._1.logType, tuple._2._2))
    })

    // save to ES
    val toEs = fullRecords.map((full: FullStreamRecord) => {

      val userAgent: UserAgent = new UserAgent(full.record.userAgent)
      val browser: Browser = userAgent.getBrowser
      val operatingSystem: OperatingSystem = userAgent.getOperatingSystem

      Map("iPinyouId" -> full.record.iPinyouId,
          "timestamp" -> full.record.timestamp,
          "biddingPrice" -> full.record.biddingPrice,
          "streamType" -> full.logType.logType,
          "uaBrowserType" -> browser.getBrowserType.getName,
          "uaBrowserName" -> browser.getGroup.name,
          "uaOsNmae" -> operatingSystem.getName,
          "uaDevice" -> operatingSystem.getDeviceType.getName,
          "city" -> full.city.city,
          "coordinate" -> s"${full.city.lalitude},${full.city.longitude}"
      )
    })

    toEs.foreachRDD { rdd =>
      val sc = rdd.context
      rdd.saveToEs(esOut)
    }

    // Save To Cassandra
    val toC = fullRecords.map((full: FullStreamRecord) => {
      val record = full.record
      val city = full.city
      CFullStreamRecord(
        record.bidId,
        record.timestamp,
        record.iPinyouId,
        record.userAgent,
        record.ip,
        record.region,
        record.adExchange,
        record.domain,
        record.url,
        record.anonymous,
        record.adSlotId,
        record.adSlotWidth,
        record.adSlotHeight,
        record.adSlotVisibility,
        record.adSlotFormat,
        record.adSlotFloorPrice,
        record.creativeId,
        record.biddingPrice,
        record.advertiser,
        full.tags.id,
        record.streamId,
        full.logType.logType,
        full.tags.tags,
        city.city,
        city.stateId,
        city.population,
        city.area,
        city.density,
        city.lalitude,
        city.longitude)
    })

    toC.foreachRDD { rdd =>
            rdd.saveToCassandra(cassandraKeyspace, cassandraTable)
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
