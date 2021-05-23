package org.apache.flink.streaming.util.recovery

import org.apache.flink.streaming.runtime.tasks.mailbox.Mail
import org.apache.flink.streaming.util.recovery.AbstractLogStorage._

import scala.collection.mutable

class DPLogManager(logWriter: AsyncLogWriter, mailResolver: MailResolver, val stepCursor: StepCursor) extends AbstractLogManager  {

  val controlQueue = new mutable.Queue[Mail]()
  val currentSender = "anywhere"
  var currentSeq = 0L
  val orderingManager = new FIFOManager[Mail, String]((s, m) => {
    controlQueue.enqueue(m)
  })

  // For recovery, only need to replay control messages, and then it's done
  logWriter.storage.getLogs.foreach {
    case ctrl: ControlRecord =>
      orderingManager.handleMessage(currentSender, currentSeq, new Mail(ctrl.controlName, ctrl.controlArgs))
      currentSeq += 1
    case other =>
    //skip
  }

  private val correlatedSeq = logWriter.storage.getLogs
    .collect {
      case DPCursor(idx) => idx
    }
    .to[mutable.Queue]


  def inputControl(mail:Mail): Unit ={
    if(!mailResolver.canHandle(mail.descriptionFormat)){
      println(s"${logWriter.storage.name} running ${mail.descriptionFormat} directly")
      mail.run()
      return
    }
    orderingManager.handleMessage(currentSender, currentSeq, mail)
    currentSeq += 1
    if(stepCursor.isRecoveryCompleted){
      while(controlQueue.nonEmpty){
        val mail = controlQueue.dequeue()
        persistCurrentControl(mail)
        println(s"${logWriter.storage.name} running ${mail.descriptionFormat} when step = ${stepCursor.getCursor}")
        mailResolver.call(mail)
        stepCursor.advance()
      }
    }
  }

  def recoverControl(): Unit ={
    while(correlatedSeq.nonEmpty && correlatedSeq.head == stepCursor.getCursor){
      correlatedSeq.dequeue()
      val mail = controlQueue.dequeue()
      println(s"${logWriter.storage.name} recovering ${mail.descriptionFormat} when step = ${stepCursor.getCursor}")
      mailResolver.call(mail)
      stepCursor.advance()
    }
  }

  def persistCurrentControl(mail:Mail): Unit = {
    logWriter.addLogRecord(ControlRecord(mail.descriptionFormat, mail.descriptionArgs))
    logWriter.addLogRecord(DPCursor(stepCursor.getCursor))
  }
}
