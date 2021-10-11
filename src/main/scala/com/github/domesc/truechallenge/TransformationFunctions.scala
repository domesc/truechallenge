package com.github.domesc.truechallenge

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, year}
import org.apache.spark.sql.types.DoubleType

object TransformationFunctions {

  /**
   * Compute the ratio of budget to revenue and extract the year of film release
   * @param df
   * @return
   */
  def addRatioAndYearToMovies(df: DataFrame): DataFrame = {
    df
      .withColumn("ratio", col("budget").cast(DoubleType)/col("revenue").cast(DoubleType))
      .withColumn("year", year(col("release_date")))
  }

  /**
   * Associate wikipedia url and abstract to each movie
   * @param movies the movies dataframe
   * @param wiki the wiki dataframe
   * @return the dataframe merged with only the interesting columns
   */
  def mergeMoviesAndWiki(movies: DataFrame, wiki: DataFrame): DataFrame = {
    movies
      .join(wiki, Seq("title"), "left")
      .select("title", "budget", "year", "revenue", "vote_average", "ratio", "production_companies", "url", "abstract")
  }

  /**
   * Compute the first N movies by ratio
   * @param df
   * @param n the number of movies to select
   * @return
   */
  def computeTopByRatio(df: DataFrame, n: Int = 1000): DataFrame ={
    df.sort(col("ratio").desc).limit(n)
  }

}
