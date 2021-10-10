package com.github.domesc.truechallenge

import com.databricks.spark.xml.XmlDataFrameReader
import com.github.domesc.truechallenge.settings.ApplicationSettings
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel

import java.io.{FileInputStream, FileWriter}
import java.util.Properties
import java.util.zip.GZIPInputStream
import scala.xml._
import scala.io.Source


object Main extends App {
  val sparkSession = SparkSession.builder().master( "local[*]" ).getOrCreate()

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

/*
  wiki.write.option("header", "true").option("delimiter", "|").csv("wiki.csv")
  val wiki = sparkSession.read
    .option( "header", "true" )
    .option("delimiter", "|")
    .csv(applicationSettings.wikiPath) */

  val moviesWithRatio = TransformationFunctions.addRatioAndYearToMovies(moviesMetadata)

  val resultJoined = TransformationFunctions.mergeMoviesAndWiki(moviesWithRatio, wiki)

  val topThousandResults = TransformationFunctions.computeTopByRatio(resultJoined)

  /* write to DB*/

  DBWriter.writeToDB(topThousandResults, applicationSettings.dbSettings, "movies")

  val connectionProperties = new Properties()
  connectionProperties.put("user", "user")
  connectionProperties.put("password", "password")
  val topFromSQL = sparkSession.read
    .option("driver", "org.postgresql.Driver")
    .jdbc("jdbc:postgresql://localhost:5432/truefilm", "movies", connectionProperties)

  topFromSQL.show(20)
}