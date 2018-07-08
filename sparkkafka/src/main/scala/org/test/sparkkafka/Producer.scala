package org.test.sparkkafka
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import scala.collection.Map
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import _root_.kafka.serializer.DefaultDecoder
import _root_.kafka.serializer._
import org.apache.spark.storage.StorageLevel

object Producer {
  def main(args: Array[String]) { 
    val conf = new SparkConf()
      .setAppName("producer")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(20))

    val kafkaConf = Map {
      "zookeeper.connect" -> "localhost:2181";
      "group.id" -> "test-consumer-group";
      "zookeeper.connection.timeout.ms" -> "5000"
    }

   val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
    ssc,
    kafkaConf,
    Map("spark-test-topic" -> 1),   // subscripe to topic and partition 1
    StorageLevel.MEMORY_ONLY
)
/*
    val text = sc.textFile("input.txt")

    text.flatMap {
      line => line.split(" ")
    }
      .map {
        word => (word, 1)
      }.reduceByKey(_ + _).saveAsTextFile("output")*/
val words = lines.flatMap{ case(x, y) => y.split(" ")}

words.print()

ssc.start()
  }
}