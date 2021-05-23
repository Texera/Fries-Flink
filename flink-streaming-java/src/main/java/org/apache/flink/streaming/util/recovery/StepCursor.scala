package org.apache.flink.streaming.util.recovery

class StepCursor(target:Long) {
  private var _cursor = 0L
  private var _callback:() => Unit = _

  def isRecoveryCompleted:Boolean = _cursor >= target

  def onComplete(callback:() => Unit):Unit = {
    _callback = callback
  }

  def advance(): Unit ={
    _cursor+=1
    if(_cursor == target && _callback != null){
      _callback()
    }
  }

  def getCursor:Long = _cursor
}
