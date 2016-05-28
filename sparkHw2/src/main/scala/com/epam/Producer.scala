package com.epam

import kafka.producer.ProducerConfig
import java.util.Properties

import kafka.producer.Producer
import kafka.producer.KeyedMessage

import scala.io.Source

object Producer {

  def main(args: Array[String]) {

    val topic = "test"

    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    Source.fromFile("D:\\zinchenko\\BDCC\\training_2016\\BigData2016\\stream.20130606-aa.txt", "iso-8859-1")
      .getLines.toStream.foreach((line: String) => {
        println(line)
        val data = new KeyedMessage[String, String](topic, line)
        producer.send(data)
        Thread.sleep(1)
    })
    producer.close()
  }

}
