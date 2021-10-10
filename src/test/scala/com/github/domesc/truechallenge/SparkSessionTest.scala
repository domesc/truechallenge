package com.github.domesc.truechallenge

import org.apache.spark.sql.SparkSession

trait SparkSessionTest {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

}
