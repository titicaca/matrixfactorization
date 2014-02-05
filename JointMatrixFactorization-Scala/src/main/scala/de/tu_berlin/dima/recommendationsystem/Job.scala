package de.tu_berlin.dima.recommendationsystem;

import java.util.Random;

import com.google.common.primitives.Longs

import org.apache.mahout.math.Vector
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable

import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.api.scala.functions._

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.scala.TextFile
import eu.stratosphere.api.scala.ScalaPlan
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.client.RemoteExecutor

// You can run this locally using:
// mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath de.tu_berlin.dima.recommendationsystem.RunJobLocal 2 file:///some/path file:///some/other/path"
object RunJobLocal {
  def main(args: Array[String]) {
    val job = new Job
    val inputPath = "file://"+System.getProperty("user.dir") +"/datasets/100k/ua.base";
    val outputPath = "file://"+System.getProperty("user.dir") +"/results/100k/Prediction_ua_i=5.result";
    val plan = job.getScalaPlan(2, inputPath, outputPath)
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

// You can run this on a cluster using:
// mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath de.tu_berlin.dima.recommendationsystem.RunJobRemote 2 file:///some/path file:///some/other/path"
object RunJobRemote {
  def main(args: Array[String]) {
    val job = new Job
    if (args.size < 3) {
      println(job.getDescription)
      return
    }
    val plan = job.getScalaPlan(args(0).toInt, args(1), args(2))
    // This will create an executor to run the plan on a cluster. We assume
    // that the JobManager is running on the local machine on the default
    // port. Change this according to your configuration.
    // You will also need to change the name of the jar if you change the
    // project name and/or version. Before running this you also need
    // to run "mvn package" to create the jar.
    val ex = new RemoteExecutor("localhost", 6123, "target/stratosphere-project-0.1-SNAPSHOT.jar");
    ex.executePlan(plan);
  }
}


/**
 * This is a outline for a Stratosphere scala job. It is actually the WordCount 
 * example from the stratosphere distribution.
 * 
 * You can run it out of your IDE using the main() method of RunJob.
 * This will use the LocalExecutor to start a little Stratosphere instance
 * out of your IDE.
 * 
 * You can also generate a .jar file that you can submit on your Stratosphere
 * cluster.
 * Just type 
 *      mvn clean package
 * in the projects root directory.
 * You will find the jar in 
 *      target/stratosphere-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
class Job extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Parameters: [numSubStasks] [input] [output]"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2))
  }


  
  def getScalaPlan(numSubTasks:Int, inputPath: String, outputPath: String) = {
    println("Job Started")
    println("InputPath: " + inputPath)
    println("OutputPath: " + outputPath)

    val tupple = DataSource(inputPath, CsvInputFormat[(Int, Int, Float)](Seq(0, 1, 2), "\n", '\t'))
    //val output = tupple.write(outputPath, CsvOutputFormat("\n", "\t"))
    
    val initItemFeatureVector = tupple groupBy { x => x._2} reduceGroup { new InitItemFeatureVectorReducer }    
    
    //val output = initItemFeatureVector.write(outputPath, CsvOutputFormat("\n", "\t"))
 
    var userFeatureVectorJoin = tupple join initItemFeatureVector where {case (a, b, c) => b} isEqualTo {case (a, b) => a} map {new Joint} 
    var userFeatureVectorReduce = userFeatureVectorJoin groupBy {case (userID, _, _, _) => userID} reduceGroup {new UserFeatureVectorUpdateReducer}
    
    var itemFeatureVectorJoin = userFeatureVectorJoin
    var itemFeatureVectorReduce = userFeatureVectorReduce
    
    println("Iteration Started")
    val numIterations = 5
    for (i <- 1 to numIterations) {
      itemFeatureVectorJoin = tupple join userFeatureVectorReduce where {case (a, b, c) => a} isEqualTo {case (a, b) => a} map {new Joint}
      itemFeatureVectorReduce = itemFeatureVectorJoin groupBy {case (_, itemID, _, _) => itemID} reduceGroup {new ItemFeatureVectorUpdateReducer}
 
      userFeatureVectorJoin = tupple join itemFeatureVectorReduce where {case (a, b, c) => b} isEqualTo {case (a, b) => a} map {new Joint} 
      userFeatureVectorReduce = userFeatureVectorJoin groupBy {case (userID, _, _, _) => userID} reduceGroup {new UserFeatureVectorUpdateReducer}
    }

    itemFeatureVectorJoin = tupple join userFeatureVectorReduce where {case (a, b, c) => a} isEqualTo {case (a, b) => a} map {new Joint}
    itemFeatureVectorReduce = itemFeatureVectorJoin groupBy {case (_, itemID, _, _) => itemID} reduceGroup {new ItemFeatureVectorUpdateReducer}
    
    val prediction = itemFeatureVectorReduce cross userFeatureVectorReduce map {new PredicetionCrosser}
    
    val output = prediction.write(outputPath, CsvOutputFormat("\n", "\t"))
  
    val plan = new ScalaPlan(Seq(output), "Rating Prediction Computation")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}
