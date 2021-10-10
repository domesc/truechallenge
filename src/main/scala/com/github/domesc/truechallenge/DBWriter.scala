package com.github.domesc.truechallenge

import com.github.domesc.truechallenge.settings.DBSettings
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

object DBWriter {

  def writeToDB(df: DataFrame, conf: DBSettings, tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", conf.username)
    connectionProperties.put("password", conf.password)
    df.write
      .mode(saveMode)
      .option("driver", conf.driver)
      .jdbc(conf.url, tableName, connectionProperties)
  }

}
