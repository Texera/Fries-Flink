package org.apache.flink.runtime.controller

import org.apache.flink.runtime.executiongraph.{ExecutionGraph, ExecutionJobVertex, ExecutionVertex, IntermediateResultPartition}
import org.apache.flink.runtime.jobgraph.{JobGraph, JobVertexID}
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher

import java.util
import java.util.TimerTask
import java.util.concurrent.CompletableFuture
import java.util.function.{BiConsumer, Consumer}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Controller {

  var repeated:Boolean = if(System.getProperty("repeatedReconfiguration") == null){
    false
  }else{
    System.getProperty("repeatedReconfiguration").toBoolean
  }

  var initialDelay:Int = if(System.getProperty("reconfInitialDelay") == null){
    5000
  }else{
    System.getProperty("reconfInitialDelay").toInt
  }

  var interval:Int = if(System.getProperty("reconfInterval") == null){
    10000
  }else{
    System.getProperty("reconfInterval").toInt
  }

  var targets:String = if(System.getProperty("reconfTargets") == null){
    ""
  }else{
    System.getProperty("reconfTargets")
  }

  var oneToManyOperators:String = if(System.getProperty("oneToMany") == null){
    ""
  }else{
    System.getProperty("oneToMany")
  }

  var scheduler:String = if(System.getProperty("reconfScheduler") == null){
    "epoch"
  }else{
    System.getProperty("reconfScheduler")
  }

  var jobCount = 0;
  def registerJobToSendControl(graph:ExecutionGraph): Unit ={
    jobCount+=1
    val jobID = "job"+jobCount;
    val t = new java.util.Timer()
    val task: TimerTask = new java.util.TimerTask {

      def getAllDownstreamWorkers(v:ExecutionVertex):util.HashSet[String] = {
        val result = new util.HashSet[String]()
        v.getProducedPartitions.values().toSeq.foreach{
          x => x.getConsumerVertexGroups.foreach{
            y => y.getAllWorkerNames.foreach(z => result.add(z))
          }
        }
        result
      }

      def convertExecutionGraphToWorkerGraph(graph:ExecutionGraph):(util.HashMap[String, util.HashSet[String]], mutable.HashMap[String, ExecutionVertex]) = {
        val result = new util.HashMap[String, util.HashSet[String]]()
        val mapping = new mutable.HashMap[String, ExecutionVertex]()
        graph.getAllVertices.foreach{
          case (_, v) =>
            v.getTaskVertices.foreach{
            w =>
              result.put(w.getTaskName+"-"+w.getParallelSubtaskIndex, getAllDownstreamWorkers(w))
              mapping(w.getTaskName+"-"+w.getParallelSubtaskIndex) = w
          }
        }
        (result, mapping)
      }

      def getVerticesAndWorkers(nameString:String):(Array[ExecutionJobVertex], Array[String]) = {
        if (nameString == ""){
          return (Array.empty, Array.empty)
        }
        val iter = graph.getVerticesTopologically.iterator()
        val names = nameString.split(",")
        val res = mutable.ArrayBuffer[ExecutionJobVertex]()
        val res2 = mutable.ArrayBuffer[String]()
        while(iter.hasNext){
          val v = iter.next()
          if(names.exists(v.getName.toLowerCase.replace(" ","").replace("=","_").contains)){
            res.append(v)
            v.getTaskVertices.foreach{
              w => res2.append(w.getTaskName+"-"+w.getParallelSubtaskIndex)
            }
          }
        }
        (res.toArray,res2.toArray)
      }

      var iteration = 0
      override def run(): Unit = {
        val currentIteration = iteration
        val innerJobID = jobID
        iteration +=1
        val futures = mutable.ArrayBuffer[CompletableFuture[_]]()
        val roundtripTimes = mutable.ListBuffer[Long]()

        val graphWithMapping = convertExecutionGraphToWorkerGraph(graph)
        val mapping = graphWithMapping._2
        val (targetVertices, targetWorkers) = getVerticesAndWorkers(targets)
        val (_, oneToManyWorkers) = getVerticesAndWorkers(oneToManyOperators)

        println("graph: "+graphWithMapping._1)
        println("target: "+targetWorkers.mkString(" "))
        println("oneToMany: "+oneToManyWorkers.mkString(" "))
        val MCS = if(scheduler != "epoch") {
          val result = FriesAlg.computeMCS(graphWithMapping._1, targetWorkers, oneToManyWorkers)
          println("MCS: "+result)
          result
        }else{
          graphWithMapping._1
        }
        val message = ControlMessage(new Consumer[Array[Object]] with Serializable {
          val vIds = targetVertices.map(_.getJobVertexId)
          override def accept(t: Array[Object]): Unit = {
            if (vIds.contains(t(0).asInstanceOf[JobVertexID])) {
              println(t(2).asInstanceOf[String] + "-" + t(1).toString + " received epoch control message!")
              System.setProperty(t(2).asInstanceOf[String] + "-" + t(1).toString, currentIteration.toString)
            }
            println(s"$innerJobID received iteration(${t(2).asInstanceOf[String]}-${t(1)}) $currentIteration time=${System.currentTimeMillis()}")
          }
        }, MCS)
        FriesAlg.getSources(MCS).foreach{
          case sourceWorkerName =>
            val worker = mapping(sourceWorkerName)
            val currentTime = System.currentTimeMillis()
            futures.append(worker.sendControlMessage(message).thenRun(new Runnable {
              override def run(): Unit = {
                val finishedTime = System.currentTimeMillis()
                roundtripTimes.append(finishedTime - currentTime)
              }
          }))
        }
        CompletableFuture.allOf(futures:_*).thenRun(new Runnable{
          override def run(): Unit = {
            println("average roundtrip time = "+ (roundtripTimes.sum/roundtripTimes.size))
          }
        })
        println(s"$jobID sent iteration $currentIteration time=${System.currentTimeMillis()}")
      }
    }
    if(repeated){
      t.schedule(task, initialDelay, interval)
    }else{
      t.schedule(task, initialDelay)
    }

  }



}
