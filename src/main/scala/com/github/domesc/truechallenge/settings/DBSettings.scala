package com.github.domesc.truechallenge.settings

import com.typesafe.config.Config

/**
 * Configuration class related to DB parameters
 * @param conf
 */
case class DBSettings(conf: Config) {

  val url = conf.getString("db.url")
  val driver = conf.getString("db.driver")
  val username = conf.getString("db.user")
  val password = conf.getString("db.password")

}
