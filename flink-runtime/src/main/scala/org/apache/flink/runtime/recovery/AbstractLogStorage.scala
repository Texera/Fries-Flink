package org.apache.flink.runtime.recovery

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo
import org.apache.flink.runtime.recovery.AbstractLogStorage.LogRecord

import scala.collection.mutable


object AbstractLogStorage{
  trait LogRecord
  case class ControlRecord(controlName:String, controlArgs:Array[Object]) extends LogRecord{
    def this() = this(null, null)
  }
  case class UpdateStepCursor(step:Long) extends LogRecord{
    def this() = this(0)
  }
  case class DPCursor(idx:Long) extends LogRecord{
    def this() = this(0)
  }
  case class ChannelOrder(inputNum:Int, newChannelID:InputChannelInfo, lastChannelRecordCount:Int) extends LogRecord{
    def this() = this(0, null,0)
  }
  case class TimerOutput(time:Long) extends LogRecord{
    def this() = this(0)
  }

  //special log messages:
  case object ShutdownWriter extends LogRecord
  case object TruncateLog extends LogRecord

}

abstract class AbstractLogStorage(val name:String) {

  def write(record:LogRecord)

  def getStepCursor:Long

  // commit all record before last commit
  def commit()

  // for recovery:
  def getLogs: Iterable[LogRecord]

  def getTimerOutputs: Array[Long]

  // delete current log and create a new one for writing
  def truncateLog():Unit

  // delete everything
  def clear(): Unit

  // release the resources
  def release(): Unit

}
