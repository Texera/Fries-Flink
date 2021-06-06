package org.apache.flink.streaming.util.recovery

import java.io.{DataInputStream, DataOutputStream}

import com.twitter.chill.akka.AkkaSerializer
import org.apache.flink.streaming.util.recovery.AbstractLogStorage._
import org.apache.flink.streaming.util.recovery.FileLogStorage._
import org.apache.hadoop.fs.{FSDataOutputStream, Syncable}

import scala.collection.mutable

object FileLogStorage {
  val globalSerializer = new AkkaSerializer(null)

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
      inputStream.readNBytes(length)
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
            case TimerOutput(time) =>
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

