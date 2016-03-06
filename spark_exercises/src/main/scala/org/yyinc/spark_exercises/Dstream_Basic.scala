package org.yyinc.spark_exercises

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Dstream_Basic {
  
  def main(Args:Array[String]){
    
    val conf=new SparkConf()
    .setAppName("Stream_Basic")
    .setMaster("local")
    
    val ssc=new StreamingContext(conf,Seconds(1))
    
    val lines=ssc.socketTextStream("localhost", 9999)
    
    val words=lines.flatMap { x => x.split(" ") }
        .map { word => (word,1) }
        .reduceByKey(_+_)
        
        words.foreachRDD({rdd =>  
          if(!rdd.partitions.isEmpty)
            rdd.saveAsTextFile(System.currentTimeMillis().toString()+ ".txt")}
        )
        
        ssc.start()
        ssc.awaitTermination()
        
      
       
    
    
  }
  
}