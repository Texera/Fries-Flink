package org.apache.flink.streaming.util.recovery

import scala.collection.mutable

/* The abstracted FIFO/exactly-once logic */
class OrderingEnforcer[T] {

  var current = 0L
  val ofoMap = new mutable.LongMap[T]

  def isDuplicated(sequenceNumber: Long): Boolean = {
    if (sequenceNumber == -1) return false
    sequenceNumber < current || ofoMap.contains(sequenceNumber)
  }

  def isAhead(sequenceNumber: Long): Boolean = {
    if (sequenceNumber == -1) return false
    sequenceNumber > current
  }

  def stash(sequenceNumber: Long, data: T): Unit = {
    ofoMap(sequenceNumber) = data
  }

  def enforceFIFO(data: T): List[T] = {
    val res = mutable.ArrayBuffer[T](data)
    current += 1
    while (ofoMap.contains(current)) {
      res.append(ofoMap(current))
      ofoMap.remove(current)
      current += 1
    }
    res.toList
  }
}
