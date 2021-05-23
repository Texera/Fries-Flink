package org.apache.flink.streaming.util.recovery

class StepCursor(target:Long) {
  private var cursor = 1L

  def isRecoveryCompleted:Boolean = cursor > target

  def advance(): Unit ={
    cursor+=1
  }

  def getCursor:Long = cursor
}
