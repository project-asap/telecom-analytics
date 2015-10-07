name := "PeakDetection"

version := "1.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.joda" % "joda-convert" % "1.8",
  "joda-time" % "joda-time" % "2.8.2"
)

unmanagedJars in Compile := (baseDirectory.value ** "*.jar").classpath
