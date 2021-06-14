package org.apache.flink.streaming.util.recovery

import java.io.{DataInputStream, DataOutputStream}

import com.twitter.chill.akka.AkkaSerializer
import org.apache.flink.runtime.recovery.{AbstractLogStorage, RecoveryUtils}
import org.apache.flink.runtime.recovery.AbstractLogStorage._
import org.apache.flink.streaming.util.recovery.FileLogStorage.{ByteArrayReader, ByteArrayWriter, _}
import org.apache.hadoop.fs.Syncable

import scala.collection.mutable

object FileLogStorage {

  class ByteArrayWriter(outputStream: DataOutputStream) {

    def write(content: Array[Byte]): Unit = {
      outputStream.writeInt(content.length)
      outputStream.write(content)
    }

    def flush(): Unit = {
      outputStream match {
        case syncable: Syncable =>
          syncable.hsync()
        case _ =>
          outputStream.flush()
      }
    }

    def close(): Unit = {
      outputStream.close()
    }
  }

  class ByteArrayReader(inputStream: DataInputStream) {

    def read(): Array[Byte] = {
      val length = inputStream.readInt()
      val res = new Array[Byte](length)
      var n = 0
      while(n < length){
        n+=inputStream.read(res, n, length - n)
      }
      res
    }

    def close(): Unit = {
      inputStream.close()
    }

    def isAvailable: Boolean = {
      inputStream.available() >= 4
    }
  }
}

abstract class FileLogStorage(logName: String) extends AbstractLogStorage(logName) {

  def getInputStream: DataInputStream

  def getOutputStream: DataOutputStream

  def fileExists: Boolean

  def createDirectories(): Unit

  def deleteFile(): Unit

  private lazy val output = new ByteArrayWriter(getOutputStream)

  private val globalSerializer = SerializeUtils.getSerializer
  private val loadedLogs = mutable.ArrayBuffer.empty[LogRecord]
  private val timerOutputs = mutable.ArrayBuffer.empty[Long]
  private var stepCursor:Long = 0L

  override def getStepCursor: Long = {
    if(loadedLogs.isEmpty){
      getLogs
    }
    stepCursor
  }

  override def getTimerOutputs: Array[Long] = {
    if(loadedLogs.isEmpty){
      getLogs
    }
    timerOutputs.toArray
  }

  override def getLogs: Iterable[LogRecord] = {
    createDirectories()
    // read file
    if (!fileExists) {
      Iterable.empty
    } else {
      if (loadedLogs.nonEmpty) {
        return loadedLogs
      }
      val input = new ByteArrayReader(getInputStream)
      while (input.isAvailable) {
        try {
          val binary = input.read()
          val message = globalSerializer.fromBinary(binary)
          message match {
            case cursor: DPCursor =>
              stepCursor = cursor.idx
              loadedLogs.append(cursor)
            case f: ChannelOrder =>
              loadedLogs.append(f)
            case ctrl: ControlRecord =>
              loadedLogs.append(ctrl)
            case payload: UpdateStepCursor =>
              stepCursor = payload.step
            case time:java.lang.Long =>
              timerOutputs.append(time)
            case other =>
              throw new RuntimeException(
                "cannot deserialize log: " + (binary.map(_.toChar)).mkString
              )
          }
        } catch {
          case e: Exception =>
            input.close()
            throw e
        }
      }
      input.close()
      loadedLogs
    }
  }

  override def write(record: LogRecord): Unit = {
    record match{
      case TimerOutput(time) =>
        output.write(globalSerializer.toBinary(Long.box(time)))
      case _ =>
        output.write(globalSerializer.toBinary(record))
    }
  }

  override def commit(): Unit = {
    output.flush()
  }

  override def clear(): Unit = {
    if (fileExists) {
      deleteFile()
    }
  }

  override def release(): Unit = {
    try {
      output.close()
    } catch {
      case e: Exception =>
        println("error occurs when closing the output: " + e.getMessage)
    }
  }

}

