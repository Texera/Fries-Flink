package org.apache.flink.streaming.util.recovery

import org.apache.flink.streaming.runtime.tasks.mailbox.Mail
import org.apache.flink.util.function.ThrowingRunnable

import scala.collection.mutable


class MailResolver {
  private val consumerHandlers = new mutable.HashMap[String, ThrowingConsumer[Array[Object],_]]()
  private val runnableHandlers = new mutable.HashMap[String, ThrowingRunnable[_]]()

  def bind(fn:String, callback:ThrowingConsumer[Array[Object],_]): Unit ={
    consumerHandlers(fn) = callback
  }

  def bind(fn:String, callback:ThrowingRunnable[_]): Unit ={
    runnableHandlers(fn) = callback
  }

  def canHandle(fn:String): Boolean ={
    consumerHandlers.contains(fn) || runnableHandlers.contains(fn)
  }

  def call(mail:Mail): Unit ={
    //println(s"running ${mail.descriptionFormat} from resolver")
    if(consumerHandlers.contains(mail.descriptionFormat)){
      consumerHandlers(mail.descriptionFormat).accept(mail.descriptionArgs)
    }else if(runnableHandlers.contains(mail.descriptionFormat)){
      runnableHandlers(mail.descriptionFormat).run()
    }
  }
}
