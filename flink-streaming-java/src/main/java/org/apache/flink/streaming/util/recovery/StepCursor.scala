package org.apache.flink.streaming.util.recovery

import org.apache.flink.streaming.util.recovery.AbstractLogStorage.UpdateStepCursor

class StepCursor(target:Long, logWriter: AsyncLogWriter) {
  private var _cursor = 0L
  private var _callback:() => Unit = _

  def isRecoveryCompleted:Boolean = _cursor >= target

  def onComplete(callback:() => Unit):Unit = {
    _callback = callback
  }

  def advance(): Unit ={
    _cursor+=1
    logWriter.addLogRecord(UpdateStepCursor(_cursor))
    if(_cursor == target && _callback != null){
      _callback()
    }
  }

  def getCursor:Long = _cursor
}
