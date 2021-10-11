package com.github.domesc.truechallenge

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}

class AppListener extends SparkListener {

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("Shutting down application ...")
  }

}
