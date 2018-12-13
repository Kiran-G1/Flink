
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.log4j._

object JoinsExample {
  def main(args: Array[String]) {
  
 val env = ExecutionEnvironment.getExecutionEnvironment
 // val env = StreamExecutionEnvironment.getExecutionEnvironment
 // this is for stream;
 val params=ParameterTool.fromArgs(args);
 env.getConfig.setGlobalJobParameters(params);
 Logger.getLogger("org").setLevel(Level.ERROR)
 
 val persons=env.readTextFile("/home/bigboy/Downloads/JOINS/person");
 persons.print()
 val tuples=persons.map{x=> 
   val a=x.split(",")
   (a(0).toInt,a(1).toString())
   
  }
 tuples.print()
 val location=env.readTextFile("/home/bigboy/Downloads/JOINS/location");
 val tuples2=location.map{x=>
   val a = x.split(",")
   (a(0).toInt,a(1).toString());
 }
 tuples2.print()
 val joined= tuples.join(tuples2).where(0) .equalTo(0)
 joined.print()
 env.execute("JoinsExample");
 }

  
  
}
