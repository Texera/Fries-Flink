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

  var controlNonstop:Boolean = if(System.getProperty("controlNonstop") == null){
    false
  }else{
    System.getProperty("controlNonstop").toBoolean
  }

  var controlInitialDelay:Int = if(System.getProperty("controlInitialDelay") == null){
    5000
  }else{
    System.getProperty("controlInitialDelay").toInt
  }

  var controlInterval:Int = if(System.getProperty("controlInterval") == null){
    10000
  }else{
    System.getProperty("controlInterval").toInt
  }

  var controlDest:String = if(System.getProperty("controlDests") == null){
    ""
  }else{
    System.getProperty("controlDests")
  }

  var controlSources:String = if(System.getProperty("controlSources") == null){
    ""
  }else{
    System.getProperty("controlSources")
  }

  var jobCount = 0;
  def registerJobToSendControl(graph:ExecutionGraph): Unit ={
    jobCount+=1
    val jobID = "job"+jobCount;
    val t = new java.util.Timer()
    val task: TimerTask = new java.util.TimerTask {

      def sendControl(v:ExecutionJobVertex, message:ControlMessage,futures:mutable.ArrayBuffer[CompletableFuture[_]], results:mutable.ListBuffer[Long]): Unit ={
        v.getTaskVertices.filter(!_.getExecutionState.isTerminal).foreach(x => {
          println(s"sending control message to $x")
          val currentTime = System.currentTimeMillis()
          futures.append(x.sendControlMessage(message).thenRun(new Runnable {
            override def run(): Unit = {
              val finishedTime = System.currentTimeMillis()
              results.append(finishedTime - currentTime)
            }
          }))
        })
      }

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
        val sources = new mutable.HashMap[String, ExecutionVertex]()
        graph.getAllVertices.foreach{
          case (_, v) =>
            if(v.getInputs.isEmpty){
              v.getTaskVertices.foreach{
                w => sources(w.getTaskName+"-"+w.getParallelSubtaskIndex) = w
              }
            }
            v.getTaskVertices.foreach{
            w =>
              result.put(w.getTaskName+"-"+w.getParallelSubtaskIndex, getAllDownstreamWorkers(w))
          }
        }
        (result, sources)
      }

      var iteration = 0;
      override def run(): Unit = {
        val iter = graph.getVerticesTopologically.iterator()
        val targetVertices = controlDest match{
          case "" =>
            var last = iter.next()
            while(iter.hasNext){
              last = iter.next()
            }
            Array(last)
          case other =>
            val names = other.split(",")
            val res = mutable.ArrayBuffer[ExecutionJobVertex]()
            while(iter.hasNext){
              val v = iter.next()
              if(names.exists(v.getName.contains)){
                res.append(v)
              }
            }
            res.toArray
        }
        println(s"triggering control message, targets = ${targetVertices.map(x => x.getName).mkString(",")}")
        val currentIteration = iteration
        val innerJobID = jobID
        iteration +=1
        val futures = mutable.ArrayBuffer[CompletableFuture[_]]()
        val roundtripTimes = mutable.ListBuffer[Long]()

        val graphWithSources = convertExecutionGraphToWorkerGraph(graph)
        val sources = graphWithSources._2

        val MCS = graphWithSources._1 //TODO: apply MCS algorithm here

        println(MCS)

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
        sources.foreach{
          case (s, w) => if(MCS.containsKey(s)){
            futures.append(w.sendControlMessage(message))
          }
        }
        CompletableFuture.allOf(futures:_*).thenRun(new Runnable{
          override def run(): Unit = {
            println("average roundtrip time = "+ (roundtripTimes.sum/roundtripTimes.size))
          }
        })
        println(s"$jobID sent iteration $currentIteration time=${System.currentTimeMillis()}")
      }
    }
    if(controlNonstop){
      t.schedule(task, controlInitialDelay, controlInterval)
    }else{
      t.schedule(task, controlInitialDelay)
    }

  }



}
