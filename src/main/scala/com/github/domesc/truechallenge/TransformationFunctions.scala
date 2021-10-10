package com.github.domesc.truechallenge

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, year}
import org.apache.spark.sql.types.DoubleType

object TransformationFunctions {

  def addRatioAndYearToMovies(df: DataFrame): DataFrame = {
    df
      .withColumn("ratio", col("budget").cast(DoubleType)/col("revenue").cast(DoubleType))
      .withColumn("year", year(col("release_date")))
  }

  def mergeMoviesAndWiki(movies: DataFrame, wiki: DataFrame): DataFrame = {
    movies
      .join(wiki, Seq("title"), "left")
      .select("title", "budget", "year", "revenue", "vote_average", "ratio", "production_companies", "url", "abstract")
  }

  def computeTopByRatio(df: DataFrame, n: Int = 1000): DataFrame ={
    df.sort(col("ratio").desc).limit(n)
  }

}
