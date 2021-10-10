package com.github.domesc.truechallenge.settings

import com.typesafe.config.Config

case class ApplicationSettings(conf: Config) {
  val dbSettings = DBSettings(conf)
  val moviesPath = conf.getString("moviesPath")
  val wikiPath = conf.getString("wikiPath")

}
