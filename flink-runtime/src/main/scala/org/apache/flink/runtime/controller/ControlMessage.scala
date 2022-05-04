package org.apache.flink.runtime.controller

import java.util
import java.util.function.Consumer

object ControlMessage{

  val FixedEpochNumber:Long = 999999999

  import java.io._

    def serialize(obj: ControlMessage): Array[Byte] = {
      val byteOut = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(byteOut)
      objOut.writeObject(obj)
      objOut.close()
      byteOut.close()
      byteOut.toByteArray
    }

    def deserialize(bytes: Array[Byte]): ControlMessage = {
      val byteIn = new ByteArrayInputStream(bytes)
      val objIn = new ObjectInputStream(byteIn)
      val obj = objIn.readObject().asInstanceOf[ControlMessage]
      byteIn.close()
      objIn.close()
      obj
    }

  var consumer:Consumer[Array[Object]] with scala.Serializable = null
}


case class ControlMessage(callback: Consumer[Array[Object]] with Serializable,
                          MCS:util.HashMap[String,util.HashSet[String]])
