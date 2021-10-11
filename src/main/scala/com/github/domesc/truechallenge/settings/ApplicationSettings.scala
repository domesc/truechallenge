package com.github.domesc.truechallenge.settings

import com.typesafe.config.Config

/**
 * Configuration class related to the whole application
 * @param conf
 */
case class ApplicationSettings(conf: Config) {
  val dbSettings = DBSettings(conf)
  val moviesPath = conf.getString("moviesPath")
  val wikiPath = conf.getString("wikiPath")

}
