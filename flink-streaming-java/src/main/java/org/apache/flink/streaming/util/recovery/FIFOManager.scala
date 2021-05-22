package org.apache.flink.streaming.util.recovery

import scala.collection.mutable

class FIFOManager[T, ID](handler: (ID, T) => Unit) {

  val orderingEnforcers = new mutable.HashMap[ID, OrderingEnforcer[T]]()

  def handleMessage(
    from: ID,
    sequenceNumber: Long,
    payload: T
  ): Unit = {
    val entry = orderingEnforcers.getOrElseUpdate(from, new OrderingEnforcer[T]())
    if (entry.isDuplicated(sequenceNumber)) {

    } else if (entry.isAhead(sequenceNumber)) {
      entry.stash(sequenceNumber, payload)
    } else {
      //logger.logInfo(s"receive $payload from $from and FIFO seq = ${entry.current}")
      entry.enforceFIFO(payload).foreach(v => handler.apply(from, v))
    }
  }

  def advanceSequenceNumber(id: ID): Unit = {
    if (!orderingEnforcers.contains(id)) {
      orderingEnforcers(id) = new OrderingEnforcer[T]()
    }
    orderingEnforcers(id).current += 1
  }

}
