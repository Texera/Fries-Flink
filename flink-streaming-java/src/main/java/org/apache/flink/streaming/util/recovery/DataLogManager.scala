package org.apache.flink.streaming.util.recovery


import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent
import org.apache.flink.streaming.util.recovery.AbstractLogStorage.{ChannelOrder, getLogStorage}
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput
import org.apache.flink.streaming.runtime.streamrecord.StreamElement
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor

import scala.collection.mutable

class DataLogManager(logWriter: AsyncLogWriter, stepCursor: StepCursor) {

  private val prevOrders = new mutable.Queue[(Int,InputChannelInfo)]()
  private val prevCounts = new mutable.Queue[Int]()
  private var lastGate:Int = -1
  private var lastChannel: InputChannelInfo = _
  private var recordCount = 0
  private var isRecovering = false

  logWriter.storage.getLogs.foreach {
    case fs: ChannelOrder =>
      isRecovering = true
      if(lastGate == -1){
        lastGate = fs.inputNum
        lastChannel = fs.newChannelID
      }else{
        prevCounts.enqueue(fs.lastChannelRecordCount)
        prevOrders.enqueue((fs.inputNum,fs.newChannelID))
      }
    case other => //skip
  }

  private val inners = new mutable.HashMap[Int, DataLogManagerInner[_]]()
  private val ofoMap = new mutable.HashMap[Int, mutable.HashMap[InputChannelInfo, mutable.Queue[Either[BufferOrEvent, (StreamElement, DataOutput[_])]]]]()
  def registerInput[T](handler:ThrowingTriConsumer[InputChannelInfo,StreamElement, DataOutput[T], _],eventHandler: ThrowingConsumer[BufferOrEvent, _]): Int ={
    val token = inners.size
    inners(token) = new DataLogManagerInner(token, handler,eventHandler)
    token
  }

  def inputData[T](token:Int, channel:InputChannelInfo, elem:StreamElement, dataOutput: DataOutput[T]): Unit ={
    if(isRecovering){
      val entry1 = ofoMap.getOrElseUpdate(token, mutable.HashMap())
      val entry2 = entry1.getOrElseUpdate(channel, mutable.Queue())
      entry2.enqueue(Right(elem, dataOutput))
      recoverUpstream()
    }else{
      preprocessInput(token, channel)
      inners(token).asInstanceOf[DataLogManagerInner[T]].inputDataRecord(channel, elem, dataOutput)
    }
  }

  def inputEvent(token:Int, channel:InputChannelInfo, elem:BufferOrEvent): Unit ={
    if(isRecovering) {
      val entry1 = ofoMap.getOrElseUpdate(token, mutable.HashMap())
      val entry2 = entry1.getOrElseUpdate(channel, mutable.Queue())
      entry2.enqueue(Left(elem))
      recoverUpstream()
    }else{
      preprocessInput(token, channel)
      inners(token).inputEvent(channel, elem)
    }
  }

  def recoverUpstream[T]():Unit = {
    while(ofoMap.contains(lastGate) && ofoMap(lastGate).contains(lastChannel) && ofoMap(lastGate)(lastChannel).nonEmpty){
      ofoMap(lastGate)(lastChannel).dequeue() match {
        case Left(a) => inners(lastGate).inputEvent(lastChannel, a)
        case Right((s,d)) => inners(lastGate).asInstanceOf[DataLogManagerInner[T]].inputDataRecord(lastChannel, s, d.asInstanceOf[DataOutput[T]])
      }
      recordCount += 1
      if(prevCounts.nonEmpty && recordCount == prevCounts.head){
        prevCounts.dequeue()
        val (g, c) = prevOrders.dequeue()
        lastGate = g
        lastChannel = c
        recordCount = 0
      }
    }
  }

  def preprocessInput(token:Int, channel:InputChannelInfo): Unit ={
    if(lastGate != token || lastChannel != channel){
      println(s"${logWriter.storage.name} pushing log record: ${ChannelOrder(token, channel, recordCount)}")
      logWriter.addLogRecord(ChannelOrder(token, channel, recordCount))
      lastGate = token
      lastChannel = channel
      recordCount = 1
    }else{
      recordCount += 1
    }
  }

  def completeRecovery(): Unit ={
    isRecovering = false
  }

  class DataLogManagerInner[T](token:Int,
                               dataHandler: ThrowingTriConsumer[InputChannelInfo,StreamElement, DataOutput[T], _],
                               eventHandler: ThrowingConsumer[BufferOrEvent, _]){

    def inputDataRecord(channel:InputChannelInfo, elem:StreamElement, output: DataOutput[T]): Unit ={
      stepCursor.cursor += 1
      println(s"${logWriter.storage.name} receive data = $elem when step = ${stepCursor.cursor} from $channel")
      dataHandler.accept(channel, elem, output)
    }

    def inputEvent(channel: InputChannelInfo, elem:BufferOrEvent): Unit ={
      stepCursor.cursor += 1
      println(s"${logWriter.storage.name} receive event = $elem when step = ${stepCursor.cursor} from $channel")
      eventHandler.accept(elem)
    }
  }

}
