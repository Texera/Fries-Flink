package org.apache.flink.runtime.controller

import org.apache.flink.api.common.JobStatus
import org.apache.flink.runtime.executiongraph.{ExecutionGraph, ExecutionJobVertex}
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.metrics.MetricNames
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore.TaskMetricStore

import java.util.TimerTask
import java.util.concurrent.CompletableFuture
import java.util.function.{BiConsumer, Consumer}
import scala.::
import scala.collection.JavaConversions.mapAsScalaMap
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
  var controlMode = if(System.getProperty("controlMode") == null){
    "subdag"
  }else{
    System.getProperty("controlMode")
  }
  var controlDest:String = if(System.getProperty("controlDest") == null){
    "process1"
  }else{
    System.getProperty("controlDest")
  }

  var controlDest2:String = if(System.getProperty("controlDest2") == null){
    "process2"
  }else{
    System.getProperty("controlDest2")
  }

  var metricCollectionInterval =  if(System.getProperty("metricCollection") == null){
    10000
  }else{
    System.getProperty("metricCollection").toInt
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
      var iteration = 0;
      override def run(): Unit = {
        var iter = graph.getVerticesTopologically.iterator()
        val targetVertex = controlDest match{
          case "final" =>
            var last = iter.next()
            while(iter.hasNext){
              last = iter.next()
            }
            last
          case other =>
            var current = iter.next()
            while(!current.getJobVertex.getName.contains(other) && iter.hasNext){
              current = iter.next()
            }
            current
        }
        println(s"${jobID} target vertex = ${targetVertex.getName}")
        var targetVertex2:ExecutionJobVertex = null
        if(controlDest2 != ""){
          val iter2 = graph.getVerticesTopologically.iterator()
          targetVertex2 = iter2.next()
          while(!targetVertex2.getJobVertex.getName.contains(controlDest2) && iter.hasNext){
            targetVertex2 = iter.next()
          }
          println(s"${jobID} target vertex2 = ${targetVertex2.getName}")
        }
        val targetExecVertex = targetVertex.getTaskVertices.head
        val vertexId = targetExecVertex.getJobvertexId
        val currentIteration = iteration
        val innerJobID = jobID
        iteration +=1
        iter = graph.getVerticesTopologically.iterator()
        val sources = new mutable.Queue[ExecutionJobVertex]
        val edgeMap = new mutable.HashMap[String,mutable.HashSet[String]]
        val nameToVertex = new mutable.HashMap[String,ExecutionJobVertex]
        while(iter.hasNext){
          val v = iter.next()
          nameToVertex(v.getName) = v
          if(v.getInputs.isEmpty){
            sources.enqueue(v)
          }
          val inputIter = v.getInputs.iterator()
          while(inputIter.hasNext){
            val upstreamName = inputIter.next().getProducer.getName
            if(!edgeMap.contains(upstreamName)){
              edgeMap(upstreamName) = new mutable.HashSet[String]
            }
            edgeMap(upstreamName).add(v.getName)
          }
        }
        controlMode match {
          case "epoch" =>
            val message = ControlMessage(new Consumer[Array[Object]] with Serializable {
              override def accept(t: Array[Object]): Unit = {
                if (t(0).asInstanceOf[JobVertexID] == vertexId) {
                  println(t(2).asInstanceOf[String] + "-" + t(1).toString + " received control message!")
                  System.setProperty(t(2).asInstanceOf[String] + "-" + t(1).toString, "true")
                }
                println(s"$innerJobID received iteration(${t(2).asInstanceOf[String]}-${t(1)}) $currentIteration time=${System.currentTimeMillis()}")
              }
            }, controlMode == "epoch")
            while (sources.nonEmpty) {
              val cand = sources.dequeue()
              val res = cand.getTaskVertices.map(e => e.sendControlMessage(message).join()).contains(true)
              if (!res) {
                edgeMap(cand.getName).foreach(x => sources.enqueue(nameToVertex(x)))
              }
            }
          case "dcm" =>
            val message = ControlMessage(new Consumer[Array[Object]] with Serializable {
              override def accept(t: Array[Object]): Unit = {
                if(t(0).asInstanceOf[JobVertexID] == vertexId){
                  println(t(2).asInstanceOf[String]+"-"+t(1).toString+" received dcm control message!")
                  System.setProperty(t(2).asInstanceOf[String]+"-"+t(1).toString, "true")
                }
                println(s"$innerJobID received iteration(${t(2).asInstanceOf[String]}-${t(1)}) $currentIteration time=${ System.currentTimeMillis()}")
              }
            }, controlMode == "epoch")
            targetVertex.getTaskVertices.foreach(x => x.sendControlMessage(message))
          case "hybrid" =>
            val vertexId2 = targetVertex2.getJobVertexId
            val epochMessage = ControlMessage(new Consumer[Array[Object]] with Serializable {
              override def accept(t: Array[Object]): Unit = {
                if(t(0).asInstanceOf[JobVertexID] == vertexId || t(0).asInstanceOf[JobVertexID] == vertexId2){
                  println(t(2).asInstanceOf[String]+"-"+t(1).toString+" received hybrid epoch control message!")
                  System.setProperty(t(2).asInstanceOf[String]+"-"+t(1).toString+"-epoch", "true")
                }
                println(s"$innerJobID received iteration(${t(2).asInstanceOf[String]}-${t(1)}) $currentIteration time=${ System.currentTimeMillis()}")
              }
            }, true)
            val dcmMessage = ControlMessage(new Consumer[Array[Object]] with Serializable {
              override def accept(t: Array[Object]): Unit = {
                if(t(0).asInstanceOf[JobVertexID] == vertexId2){
                  println(t(2).asInstanceOf[String]+"-"+t(1).toString+" received hybrid dcm control message!")
                  System.setProperty(t(2).asInstanceOf[String]+"-"+t(1).toString+"-dcm", "true")
                }
                println(s"$innerJobID received iteration(${t(2).asInstanceOf[String]}-${t(1)}) $currentIteration time=${ System.currentTimeMillis()}")
              }
            }, false)
            CompletableFuture.allOf(targetVertex.getTaskVertices.map(x => x.sendControlMessage(epochMessage)):_*).thenRun(new Runnable {
              override def run(): Unit = {
                targetVertex2.getTaskVertices.foreach(y => y.sendControlMessage(dcmMessage))
              }
            })
          case "subdag" =>
            val vertexId2 = targetVertex2.getJobVertexId
            val epochMessage = ControlMessage(new Consumer[Array[Object]] with Serializable {
              override def accept(t: Array[Object]): Unit = {
                if(t(0).asInstanceOf[JobVertexID] == vertexId2){
                  println(t(2).asInstanceOf[String]+"-"+t(1).toString+" received subdag control message!")
                  System.setProperty(t(2).asInstanceOf[String]+"-"+t(1).toString, "true")
                }
                println(s"$innerJobID received iteration(${t(2).asInstanceOf[String]}-${t(1)}) $currentIteration time=${ System.currentTimeMillis()}")
              }
            }, true)
            targetVertex.getTaskVertices.map(x => x.sendControlMessage(epochMessage))
          case other =>
        }
        println(s"$jobID sent iteration $currentIteration time=${System.currentTimeMillis()}")
      }
    }
    if(controlNonstop){
      t.schedule(task, controlInitialDelay, controlInterval)
    }else{
      t.schedule(task, controlInitialDelay)
    }


    def printMetrics(taskName: String, taskMetricStore: TaskMetricStore, metricName:String): Unit ={
      val values = taskMetricStore.getAllSubtaskMetricStores.map{
        case (mk, mv) =>
          mv.getMetric(metricName,"0").toDouble
      }
      println(s"${jobID} --- $taskName --- $metricName {avg: ${values.sum/ values.size} max: ${values.max} min: ${values.min}")
    }

    val metricsCollection = new TimerTask {
      override def run(): Unit = {
        if(metricFetcher == null) return
        metricFetcher.update()
        graph.getAllVertices.foreach{
          case (k,v) =>{
            val taskStore = metricFetcher.getMetricStore.getTaskMetricStore(graph.getJobID.toString, k.toString)
            if(taskStore != null){
              printMetrics(v.getName, taskStore, MetricNames.IO_NUM_RECORDS_IN_RATE)
              printMetrics(v.getName, taskStore, MetricNames.IO_NUM_RECORDS_OUT_RATE)
              printMetrics(v.getName, taskStore, MetricNames.IO_NUM_RECORDS_IN)
              printMetrics(v.getName, taskStore, MetricNames.IO_NUM_RECORDS_OUT)
            }
          }
        }
      }
    }

    t.schedule(metricsCollection, metricCollectionInterval, metricCollectionInterval)

    graph.getTerminationFuture.thenAccept(new Consumer[JobStatus] {
      override def accept(t: JobStatus): Unit = {
        task.cancel()
        metricsCollection.cancel()
      }
    })

  }



}
