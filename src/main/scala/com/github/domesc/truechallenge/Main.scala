package com.github.domesc.truechallenge

import com.databricks.spark.xml.XmlDataFrameReader
import com.github.domesc.truechallenge.settings.ApplicationSettings
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Main extends App {
  val sparkSession = SparkSession.builder().master( "local[*]" ).getOrCreate()
  sparkSession.sparkContext.addSparkListener(new AppListener())

  val applicationSettings = ApplicationSettings(ConfigFactory.load())

  val moviesMetadata = sparkSession.read
    .option( "header", "true" )
    .option("inferSchema", "true")
    .option("escape", "\"")
    .csv(applicationSettings.moviesPath)

  val wiki = sparkSession.read
    .option("rowTag", "doc")
    .xml(applicationSettings.wikiPath)
    .select("title", "url", "abstract")
    .withColumn("title", regexp_replace(col("title"), "Wikipedia: ", ""))

  val moviesWithRatio = TransformationFunctions.addRatioAndYearToMovies(moviesMetadata)

  val resultJoined = TransformationFunctions.mergeMoviesAndWiki(moviesWithRatio, wiki)

  val topThousandResults = TransformationFunctions.computeTopByRatio(resultJoined)

  /* write to DB*/

  DBHandler.writeToDB(topThousandResults, applicationSettings.dbSettings, "movies")
}