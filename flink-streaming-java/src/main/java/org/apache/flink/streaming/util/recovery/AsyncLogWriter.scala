package org.apache.flink.streaming.util.recovery

import java.util
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue}

import org.apache.flink.shaded.guava18.com.google.common.collect.Queues
import org.apache.flink.streaming.util.recovery.AbstractLogStorage._

class AsyncLogWriter(val storage:AbstractLogStorage) {
  private var persistedStepCursor = storage.getStepCursor
  private var cursorUpdated = false
  val logRecordQueue: LinkedBlockingQueue[LogRecord] = Queues.newLinkedBlockingQueue()

  def addLogRecord(logRecord: LogRecord): Unit = {
      logRecordQueue.put(logRecord)
  }

  private val loggingExecutor: ExecutorService = Executors.newSingleThreadExecutor
    loggingExecutor.submit(new Runnable() {
      def run(): Unit = {
        try {
          Thread.currentThread().setPriority(Thread.MAX_PRIORITY)
          var isEnded = false
          val buffer = new util.ArrayList[LogRecord]()
          while (!isEnded) {
            logRecordQueue.drainTo(buffer)
            //if(storage.name == "exampleJob-0f02-0")System.out.println("drained = "+buffer.size())
            var start = 0L
            if (buffer.isEmpty) {
              // instead of using Thread.sleep(200),
              // we wait until 1 record has been pushed into the queue
              // then write this record and commit
              // during this process, we accumulate log records in the queue
              val logRecord = logRecordQueue.take()
              start = System.currentTimeMillis()
              cursorUpdated = false //invalidate flag
              writeLogRecord(logRecord)
              persistStepCursor()
              storage.commit()
            } else {
              if (buffer.get(buffer.size() - 1) == ShutdownWriter) {
                buffer.remove(buffer.size() - 1)
                isEnded = true
              }
              start = System.currentTimeMillis()
              cursorUpdated = false // invalidate flag
              batchWrite(buffer)
              persistStepCursor()
              //println(s"writing ${buffer.size} logs at a time")
              buffer.clear()
            }
            //if(storage.name == "exampleJob-0f02-0") System.out.println("buffer size = "+logRecordQueue.size())
          }
          storage.release()
        } catch {
          case e: Throwable =>
            e.printStackTrace()
        }
      }
    })

  private def batchWrite(buffer: util.ArrayList[LogRecord]): Unit = {
    buffer.toArray(Array[LogRecord]()).foreach(x => writeLogRecord(x))
    storage.commit()
  }

  private def writeLogRecord(record: LogRecord): Unit = {
    record match {
      case clr: ControlRecord =>
        storage.write(clr)
      case cursor: DPCursor =>
        persistedStepCursor = cursor.idx
        storage.write(cursor)
      case record: ChannelOrder =>
        storage.write(record)
      case UpdateStepCursor(cursor) =>
        //only write the last step cursor of batched log entries
        if(cursor > persistedStepCursor){
          cursorUpdated = true
          persistedStepCursor = cursor
        }
      case t:TimerOutput =>
        storage.write(t)
      case ShutdownWriter =>
      //skip
    }
  }

  private def persistStepCursor(): Unit ={
    if(cursorUpdated){
      storage.write(UpdateStepCursor(persistedStepCursor))
    }
  }

}
