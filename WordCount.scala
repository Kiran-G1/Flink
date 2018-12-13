import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala._
import org.apache._

import org.apache.flink.streaming.api.scala._
import org.apache.flink._
import org.apache.flink.api.common.ExecutionConfig._
import org.apache.flink.api.common.time._
import org.apache._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.common.typeinfo.Types

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.wordcount.util.WordCountData
import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.utils._
import org.apache.flink.util.Collector
object WordCount {


  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment
   //val env = StreamExecution.getExecutionEnvironment
   
    //Env provides methods to control job execution
    
    val params= ParameterTool.fromArgs(args);
    //params provide utility methods to read args;
    
    env.getConfig.setGlobalJobParameters(params);
    //registering params globally for all clusters;
    
    
    val text=env.readTextFile("/home/kiran/Music/word count/wc.txt")
    
    val filtered= text.filter(x=>x.startsWith("N")).map(x=>(x,1))
    
    val counts=filtered.groupBy(0).sum(1);
    
    if(params.has("output"))
    {
    counts.writeAsCsv(params.get("/home/kiran/Music/"),"\n"," ")
    env.execute("WordCount")
    }

}

 
}
