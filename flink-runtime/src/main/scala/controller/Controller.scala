package controller

import org.apache.flink.api.common.JobStatus
import org.apache.flink.runtime.checkpoint.CheckpointOptions
import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID

import java.util.TimerTask
import java.util.function.Consumer

object Controller {

  val controlInterval = 10000
  val controlMode = "epoch"
  val controlDest:String = "final"

  def registerJobToSendControl(graph:ExecutionGraph): Unit ={
    val t = new java.util.Timer()
    val task: TimerTask = new java.util.TimerTask {
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
        val message = ControlMessage(new Consumer[Array[Object]] with Serializable {
          override def accept(t: Array[Object]): Unit = {
            println(t(0).asInstanceOf[JobVertexID] == vertexId)
            println(t(1).asInstanceOf[Int] == idx)
            if (t(0).asInstanceOf[JobVertexID] == vertexId && t(1).asInstanceOf[Int] == idx) {
              println(System.currentTimeMillis())
            }
          }
        }, controlMode == "epoch")
        controlMode match{
          case "epoch" =>
            graph.getAllExecutionVertices.iterator().next().sendControlMessage(message)
          case "dcm" =>
            targetVertex.getTaskVertices.head.sendControlMessage(message)
          case other =>
        }
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
