package com.github.domesc.truechallenge


import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConversions._

class TransformationFunctionsSpec extends AnyFlatSpec with Matchers with SparkSessionTest {
  behavior of "TransformationFunctions"

  it should "add ratio and year to movies df" in {
    val schema = StructType(Array(
      StructField("budget", StringType, true),
      StructField("revenue", StringType, true),
      StructField("release_date", StringType, true)
    ))
    val data = Seq(
      Row("10000", "20000", "2012-05-04"),
      Row("15000", "20000", "2014-05-04")
    )
    val df = spark.createDataFrame(data, schema)
    val resultDf = TransformationFunctions.addRatioAndYearToMovies(df)
    val result = resultDf.collect()
    val resultSchema = StructType(Array(
      StructField("budget", StringType, true),
      StructField("revenue", StringType, true),
      StructField("release_date", StringType, true),
      StructField("ratio", DoubleType, true),
      StructField("year", IntegerType, true)
    ))

    resultSchema shouldEqual resultSchema
    result should contain theSameElementsAs Seq(
      Row("10000", "20000", "2012-05-04", 0.5, 2012),
      Row("15000", "20000", "2014-05-04", 0.75, 2014)
    )
  }

  it should "merge movies and wiki" in {
    val movieSchema = StructType(Array(
      StructField("title", StringType, true),
      StructField("budget", StringType, true),
      StructField("year", IntegerType, true),
      StructField("revenue", StringType, true),
      StructField("vote_average", DoubleType, true),
      StructField("ratio", DoubleType, true),
      StructField("production_companies", StringType, true)
    ))
    val movies = Seq(
      Row("The Matrix", "10000", 2002, "20000", 5.3, 0.5, "blabla"),
      Row("The Matrix 2", "15000", 2010, "20000", 2.3, 0.75, "blabla2")
    )
    val dfMovies = spark.createDataFrame(movies, movieSchema)

    val wikiSchema = StructType(Array(
      StructField("title", StringType, true),
      StructField("url", StringType, true),
      StructField("abstract", StringType, true)
    ))

    val wiki = Seq(
      Row("The Matrix", "www.matrix.com", "Great film"),
      Row("The Matrix 2", "www.matrix2.com", "Not that great"),
      Row("I don't know", "www.idontknow.com", "I have no idea")
    )
    val dfWiki = spark.createDataFrame(wiki, wikiSchema)
    val resultDf = TransformationFunctions.mergeMoviesAndWiki(dfMovies, dfWiki)
    val result = resultDf.collect()
    val schemaResult = StructType(movieSchema ++ Seq(
      StructField("url", StringType, true),
      StructField("abstract", StringType, true)
    ))
    schemaResult shouldEqual resultDf.schema
    result should contain theSameElementsAs Seq(
      Row("The Matrix", "10000", 2002, "20000", 5.3, 0.5, "blabla", "www.matrix.com", "Great film"),
      Row("The Matrix 2", "15000", 2010, "20000", 2.3, 0.75, "blabla2", "www.matrix2.com", "Not that great")
    )
  }

  it should "sort descending by ratio" in {
    val ratioSchema = StructType(Array(
      StructField("ratio", DoubleType, true)
    ))
    val ratios = Seq(
      Row(0.5),
      Row(0.75),
      Row(2.0),
      Row(8.5),
      Row(5.6),
      Row(5.5),
      Row(9.5)
    )
    val dfRatios = spark.createDataFrame(ratios, ratioSchema)

    val resultDf = TransformationFunctions.computeTopByRatio(dfRatios, 5)

    resultDf.collect() shouldEqual Array(Row(9.5), Row(8.5), Row(5.6), Row(5.5), Row(2.0))
  }


}
