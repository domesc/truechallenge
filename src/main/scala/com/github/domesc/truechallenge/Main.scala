package com.github.domesc.truechallenge

import com.databricks.spark.xml.XmlDataFrameReader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import java.io.{FileInputStream, FileWriter}
import java.util.Properties
import java.util.zip.GZIPInputStream
import scala.xml._
import scala.io.Source


object Main extends App {
  val sparkSession = SparkSession.builder().master( "local[*]" ).getOrCreate()
  val moviesMetadata = sparkSession.read
    .option( "header", "true" )
    .option("inferSchema", "true")
    .option("escape", "\"")
    .csv( "movies_metadata.csv")
  /*val wiki = sparkSession.read
    .option("rowTag", "doc")
    .xml("enwiki-latest-abstract.xml")
    .select("title", "url", "abstract")
    .withColumn("title", regexp_replace(col("title"), "Wikipedia: ", ""))

  wiki.write.option("header", "true").option("delimiter", "|").csv("wiki.csv")*/
  val wiki = sparkSession.read
    .option( "header", "true" )
    .option("delimiter", "|")
    .csv( "wiki.csv" )

  val moviesWithRatio = moviesMetadata
    .withColumn("ratio", col("budget").cast(DoubleType)/col("revenue").cast(DoubleType))
    .withColumn("year", year(col("release_date")))

  val resultJoined = moviesWithRatio
    .join(wiki, Seq("title"), "left")
    .select("title", "budget", "year", "revenue", "vote_average", "ratio", "production_companies", "url", "abstract")

  import org.apache.spark.sql.expressions.Window
  val window = Window.partitionBy( "title").orderBy( col("ratio").desc)
  val topThousandResults = resultJoined.sort(col("ratio").desc).limit(1000)


  /* write to DB*/
  val connectionProperties = new Properties()
  connectionProperties.put("user", "user")
  connectionProperties.put("password", "password")

  topThousandResults.write
    .option("driver", "org.postgresql.Driver")
    .jdbc("jdbc:postgresql://localhost:5432/truefilm", "movies", connectionProperties)

  val topFromSQL = sparkSession.read
    .option("driver", "org.postgresql.Driver")
    .jdbc("jdbc:postgresql://localhost:5432/truefilm", "movies", connectionProperties)

  topFromSQL.show(20)


  import sparkSession.implicits._
  /*val moviesTotal = moviesMetadata.count()
  val moviesUniques = moviesMetadata.select(col("imdb_id")).distinct().count()


  moviesMetadata.printSchema()
  val head = moviesMetadata.limit( 1 ).collect()
  moviesMetadata.columns.zip(head).foreach{ case (m, h) => println(s"$m $h")}*/
}