ThisBuild / scalaVersion := "2.12.18"

name := "meridian-batch"
version := "0.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.9" % Provided
