package org.apache.flink.runtime.controller

import org.apache.flink.api.common.JobStatus
import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.metrics.MetricNames
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore.TaskMetricStore

import java.util.TimerTask
import java.util.function.{BiConsumer, Consumer}
import scala.collection.JavaConversions.mapAsScalaMap

object Controller {
  private var metricFetcher: MetricFetcher = null

  var controlNonstop:Boolean = if(System.getProperty("controlNonstop") == null){
    true
  }else{
    System.getProperty("controlNonstop").toBoolean
  }

  var controlInitialDelay:Int = if(System.getProperty("controlInitialDelay") == null){
    10000
  }else{
    System.getProperty("controlInitialDelay").toInt
  }

  var controlInterval:Int = if(System.getProperty("controlInterval") == null){
    10000
  }else{
    System.getProperty("controlInterval").toInt
  }
  var controlMode = if(System.getProperty("controlMode") == null){
    "epoch"
  }else{
    System.getProperty("controlMode")
  }
  var controlDest:String = if(System.getProperty("controlDest") == null){
    "final"
  }else{
    System.getProperty("controlDest")
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
        val iter = graph.getVerticesTopologically.iterator()
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
        println(s"${jobID} target vextex = ${targetVertex.getName}")
        val targetExecVertex = targetVertex.getTaskVertices.head
        val vertexId = targetExecVertex.getJobvertexId
        val idx = targetExecVertex.getID.getSubtaskIndex
        val currentIteration = iteration
        val innerJobID = jobID
        iteration +=1
        val message = ControlMessage(new Consumer[Array[Object]] with Serializable {
          override def accept(t: Array[Object]): Unit = {
            if(t(0).asInstanceOf[JobVertexID] == vertexId){
              println(t(2).asInstanceOf[String]+"-"+t(1).toString+" received control message!")
              System.setProperty(t(2).asInstanceOf[String]+"-"+t(1).toString, "true")
            }
            println(s"$innerJobID received iteration(${t(2).asInstanceOf[String]}-${t(1)}) $currentIteration time=${ System.currentTimeMillis()}")
          }
        }, controlMode == "epoch")
        controlMode match{
          case "epoch" =>
            val iter = graph.getVerticesTopologically.iterator()
            while(iter.hasNext){
              val v = iter.next()
              if(v.getInputs.isEmpty){
                v.getTaskVertices.foreach(e => e.sendControlMessage(message))
              }
            }
          case "dcm" =>
            targetVertex.getTaskVertices.foreach(x => x.sendControlMessage(message))
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
        graph.getAllVertices.foreach{
          case (k,v) =>{
            val taskStore = metricFetcher.getMetricStore.getTaskMetricStore(graph.getJobID.toString, k.toString)
            if(taskStore != null){
              printMetrics(v.getName, taskStore, MetricNames.IO_NUM_RECORDS_IN_RATE)
              printMetrics(v.getName, taskStore, MetricNames.IO_NUM_RECORDS_OUT_RATE)
              printMetrics(v.getName, taskStore, MetricNames.IO_NUM_BYTES_IN_RATE)
              printMetrics(v.getName, taskStore, MetricNames.IO_NUM_BYTES_OUT_RATE)
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
