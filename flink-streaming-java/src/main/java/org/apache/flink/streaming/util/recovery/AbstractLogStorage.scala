package org.apache.flink.streaming.util.recovery

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo
import org.apache.flink.streaming.util.recovery.AbstractLogStorage.LogRecord


object AbstractLogStorage{
  trait LogRecord
  case class ControlRecord(controlName:String, controlArgs:Array[Object]) extends LogRecord
  case class UpdateStepCursor(step:Long) extends LogRecord
  case class DPCursor(idx:Long) extends LogRecord
  case class ChannelOrder(inputNum:Int, newChannelID:InputChannelInfo, lastChannelRecordCount:Int) extends LogRecord
  case object ShutdownWriter extends LogRecord


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

  // delete everything
  def clear(): Unit

  // release the resources
  def release(): Unit

}
