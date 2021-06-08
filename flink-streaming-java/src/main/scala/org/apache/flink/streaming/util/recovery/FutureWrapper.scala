package org.apache.flink.streaming.util.recovery

import java.util.concurrent.CompletableFuture

class FutureWrapper {

  private var future:CompletableFuture[Void] = _

  def get(): Unit ={
    if(future!=null){
      future.get()
    }
  }

  def reset(): Unit ={
    future = new CompletableFuture[Void]()
  }

  def set(): Unit ={
    if(future != null){
      future.complete(null)
    }
  }

}
