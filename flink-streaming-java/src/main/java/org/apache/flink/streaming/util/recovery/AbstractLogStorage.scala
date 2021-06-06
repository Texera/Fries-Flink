package org.apache.flink.streaming.util.recovery

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo
import org.apache.flink.streaming.util.recovery.AbstractLogStorage.LogRecord

import scala.collection.mutable


object AbstractLogStorage{
  trait LogRecord
  case class ControlRecord(controlName:String, controlArgs:Array[Object]) extends LogRecord
  case class UpdateStepCursor(step:Long) extends LogRecord
  case class DPCursor(idx:Long) extends LogRecord
  case class ChannelOrder(inputNum:Int, newChannelID:InputChannelInfo, lastChannelRecordCount:Int) extends LogRecord
  case object ShutdownWriter extends LogRecord
  case class TimerOutput(time:Long) extends LogRecord


  def getLogStorage(name:String):AbstractLogStorage = {
    new LocalDiskLogStorage(name)
  }

}

abstract class AbstractLogStorage(val name:String) {

  def write(record:LogRecord)

  def getStepCursor:Long

  // commit all record before last commit
  def commit()

  // for recovery:
  def getLogs: Iterable[LogRecord]

  def getTimerOutputs: Array[Long]

  // delete everything
  def clear(): Unit

  // release the resources
  def release(): Unit

}
