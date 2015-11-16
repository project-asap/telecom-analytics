name := "PeakDetection"

version := "1.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-hive" % "1.5.0",
  "org.joda" % "joda-convert" % "1.8",
  "joda-time" % "joda-time" % "2.8.2",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

unmanagedJars in Compile := (baseDirectory.value ** "*.jar").classpath
