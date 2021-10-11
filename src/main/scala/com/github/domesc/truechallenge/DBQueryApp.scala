package com.github.domesc.truechallenge

import com.github.domesc.truechallenge.settings.ApplicationSettings
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object DBQueryApp extends App {
  implicit val sparkSession = SparkSession.builder().master( "local[*]" ).getOrCreate()
  sparkSession.sparkContext.addSparkListener(new AppListener())

  val applicationSettings = ApplicationSettings(ConfigFactory.load())

  val table = DBHandler.readTable(applicationSettings.dbSettings, "movies")
  table.createOrReplaceTempView("movies")

  while(true){
    println("Write your query to the movies table:")
    val query = scala.io.StdIn.readLine()
    val df = sparkSession.sql(query)
    df.show(1000)
  }

}
