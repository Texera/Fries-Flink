package controller

import org.apache.flink.api.common.JobStatus
import org.apache.flink.runtime.checkpoint.CheckpointOptions
import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID

import java.util.TimerTask
import java.util.function.Consumer

object Controller {

  val controlInterval:Int = if(System.getProperty("controlInterval") == null){
    10000
  }else{
    System.getProperty("controlInterval").toInt
  }
  val controlMode = if(System.getProperty("controlMode") == null){
    "epoch"
  }else{
    System.getProperty("controlMode")
  }
  val controlDest:String = if(System.getProperty("controlDest") == null){
    "final"
  }else{
    System.getProperty("controlDest")
  }

  def registerJobToSendControl(graph:ExecutionGraph): Unit ={
    val t = new java.util.Timer()
    val task: TimerTask = new java.util.TimerTask {
      var iteration = 0;
      override def run(): Unit = {
        val targetVertex = controlDest match{
          case "final" =>
            val iter = graph.getVerticesTopologically.iterator()
            var last = iter.next()
            while(iter.hasNext){
              last = iter.next()
            }
            last
        }
        val targetExecVertex = targetVertex.getTaskVertices.head
        val vertexId = targetExecVertex.getJobvertexId
        val idx = targetExecVertex.getID.getSubtaskIndex
        val currentIteration = iteration
        iteration +=1
        val message = ControlMessage(new Consumer[Array[Object]] with Serializable {
          override def accept(t: Array[Object]): Unit = {
            if (t(0).asInstanceOf[JobVertexID] == vertexId && t(1).asInstanceOf[Int] == idx) {
              println(s"received iteration $currentIteration time=${ System.currentTimeMillis()}")
            }
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
            targetVertex.getTaskVertices.head.sendControlMessage(message)
          case other =>
        }
        println(s"sent iteration $currentIteration time=${System.currentTimeMillis()}")
      }
    }
    t.schedule(task, 10000, controlInterval)
    graph.getTerminationFuture.thenAccept(new Consumer[JobStatus] {
      override def accept(t: JobStatus): Unit = {
        task.cancel()
      }
    })

  }



}
