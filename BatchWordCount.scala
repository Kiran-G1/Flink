package com.kamali.exe

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.log4j._
object WordCount {
  
def main(args: Array[String]) {
  
  
  
 val env = ExecutionEnvironment.getExecutionEnvironment
 // val env = StreamExecutionEnvironment.getExecutionEnvironment
 // this is for stream;
 val params=ParameterTool.fromArgs(args);
 env.getConfig.setGlobalJobParameters(params);


 Logger.getLogger("org").setLevel(Level.ERROR)
 
 
  val text=env.readTextFile("/home/bigboy/Downloads/word-count/word count/wc.txt")
  
  val filtered= text.filter(x=>x.startsWith("N"))
  val tokenized=filtered.flatMap(x=>Iterable((x,1)))
  val counts1=tokenized.groupBy(0)
  val counts2=counts1.sum(1)
  counts2.writeAsCsv("/home/bigboy/","\n"," ")
  
  env.execute("WordCount Example"); //this line is important; Without this main function won't execute;
  
  
  
  
  
      
  
 
}

}
