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

  createDirectories()

  private var output = new ByteArrayWriter(getOutputStream)

  private val globalSerializer = SerializeUtils.getSerializer
  private val loadedLogs = mutable.ArrayBuffer.empty[LogRecord]
  private val timerOutputs = mutable.ArrayBuffer.empty[Long]
  private var stepCursor:Long = 0L
  private var lastTime = 0L

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

  override def truncateLog(): Unit = {
    output.close()
    deleteFile()
    output = new ByteArrayWriter(getOutputStream)
  }

  override def getLogs: Iterable[LogRecord] = {
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
          val message = globalSerializer.fromBytes(binary)
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
              lastTime = time
            case t:java.lang.Integer =>
              lastTime += t
              timerOutputs.append(lastTime)
            case t:java.lang.Short =>
              lastTime += t
              timerOutputs.append(lastTime)
            case t:java.lang.Byte =>
              lastTime += t
              timerOutputs.append(lastTime)
            case other =>
              throw new RuntimeException(
                "cannot deserialize log: " + (binary.map(_.toChar)).mkString
              )
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
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
        if(time - lastTime <= Byte.MaxValue){
          output.write(globalSerializer.toBytesWithClass(Byte.box((time-lastTime).toByte)))
        }else if(time -lastTime <= Short.MaxValue){
          output.write(globalSerializer.toBytesWithClass(Short.box((time-lastTime).toShort)))
        }else if(time - lastTime <= Int.MaxValue){
          output.write(globalSerializer.toBytesWithClass(Int.box((time-lastTime).toInt)))
        }else{
          output.write(globalSerializer.toBytesWithClass(Long.box(time)))
        }
        lastTime = time
      case _ =>
        output.write(globalSerializer.toBytesWithClass(record))
    }
  }

  override def commit(): Unit = {
    output.flush()
  }

  override def clear(): Unit = {
    if (fileExists) {
      output.close()
      deleteFile()
      output = new ByteArrayWriter(getOutputStream)
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

