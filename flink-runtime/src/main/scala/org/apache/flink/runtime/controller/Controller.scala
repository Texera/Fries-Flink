package org.apache.flink.runtime.controller

import org.apache.flink.runtime.executiongraph.{ExecutionGraph, ExecutionJobVertex}
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher

import java.util
import java.util.TimerTask
import java.util.concurrent.CompletableFuture
import java.util.function.{BiConsumer, Consumer}
import scala.collection.mutable

object Controller {
  private var metricFetcher: MetricFetcher = null

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

  def setMetricFetcher(m:MetricFetcher): Unit ={
    metricFetcher = m
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

      var iteration = 0;
      override def run(): Unit = {
        var iter = graph.getVerticesTopologically.iterator()
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
        iter = graph.getVerticesTopologically.iterator()
        val edgeMap = new mutable.HashMap[String,mutable.HashSet[String]]
        val nameToVertex = new mutable.HashMap[String,ExecutionJobVertex]
        while(iter.hasNext){
          val v = iter.next()
          nameToVertex(v.getName) = v
          val inputIter = v.getInputs.iterator()
          while(inputIter.hasNext){
            val upstreamName = inputIter.next().getProducer.getName
            if(!edgeMap.contains(upstreamName)){
              edgeMap(upstreamName) = new mutable.HashSet[String]
            }
            edgeMap(upstreamName).add(v.getName)
          }
        }
        val vIds = targetVertices.map(_.getJobVertexId)
        val futures = mutable.ArrayBuffer[CompletableFuture[_]]()
        val roundtripTimes = mutable.ListBuffer[Long]()
        val vId = vIds.head


        val MCS = new util.HashMap[String, util.HashSet[String]]()
        val numTargetSubTasks = new util.HashMap[String, Integer]()


        val message = ControlMessage(new Consumer[Array[Object]] with Serializable {
          override def accept(t: Array[Object]): Unit = {
            if (vId == (t(0).asInstanceOf[JobVertexID])) {
              println(t(2).asInstanceOf[String] + "-" + t(1).toString + " received epoch control message!")
              System.setProperty(t(2).asInstanceOf[String] + "-" + t(1).toString, currentIteration.toString)
            }
            println(s"$innerJobID received iteration(${t(2).asInstanceOf[String]}-${t(1)}) $currentIteration time=${System.currentTimeMillis()}")
          }
        }, MCS, numTargetSubTasks)
        val startVs = controlSources.split(",").map(_.toLowerCase)
        val graphIter = graph.getVerticesTopologically.iterator()
        while(graphIter.hasNext){
          val v = graphIter.next()
          if(startVs.exists(v.getName.replace(" ","").replace("=","-").toLowerCase.contains)){
            sendControl(v, message, futures, roundtripTimes)
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
