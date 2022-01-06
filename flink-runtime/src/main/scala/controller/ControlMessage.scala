package controller

import java.util.function.Consumer

case class ControlMessage(callback: Consumer[Array[Object]])
