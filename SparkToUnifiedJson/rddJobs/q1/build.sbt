name := "Q1 RDD Spark Job"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided",
  "joda-time" % "joda-time" % "2.10.2")

assemblyJarName in assembly := "q1.jar"
