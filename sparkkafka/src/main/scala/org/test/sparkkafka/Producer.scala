package org.test.sparkkafka

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Producer {
  def main(args: Array[String])
  {
    val conf = new SparkConf()
    .setAppName("producer")
    .setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val text= sc.textFile("input.txt")
    
    text.flatMap{
      line => line.split(" ")
    }
    .map {
      word => (word,1)
          }.reduceByKey(_+_).saveAsTextFile("output")
    
  }
}