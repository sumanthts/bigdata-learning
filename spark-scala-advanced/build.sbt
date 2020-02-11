name := "spark-scala-advanced"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion