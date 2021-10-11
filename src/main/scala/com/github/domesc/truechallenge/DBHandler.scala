package com.github.domesc.truechallenge

import com.github.domesc.truechallenge.settings.DBSettings
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object DBHandler {

  /**
   * Generic way to write to a certain DB from Spark
   * @param df
   * @param conf the conf containing the [[DBSettings]]
   * @param tableName the name of the table to create
   * @param saveMode the save mode Overwrite is default
   */
  def writeToDB(df: DataFrame, conf: DBSettings, tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", conf.username)
    connectionProperties.put("password", conf.password)
    df.write
      .mode(saveMode)
      .option("driver", conf.driver)
      .jdbc(conf.url, tableName, connectionProperties)
  }

  /**
   * Read a certain table from DB
   * @param conf
   * @param tableName
   * @param sparkSession
   * @return
   */
  def readTable(conf: DBSettings, tableName: String)(implicit sparkSession: SparkSession): DataFrame = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", conf.username)
    connectionProperties.put("password", conf.password)
    sparkSession.read
      .option("driver", conf.driver)
      .jdbc(conf.url, tableName, connectionProperties)

  }

}
