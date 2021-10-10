name := "truechallenge"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "com.typesafe" % "config" % "1.4.1",
  "com.databricks" %% "spark-xml" % "0.13.0",
  "org.postgresql" % "postgresql" % "42.2.24",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test"
)

