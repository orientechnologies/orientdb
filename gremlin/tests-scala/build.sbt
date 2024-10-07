name := "orientdb-tp3-test"
organization := "com.orientechnologies"
version := "1.0.0-SNAPSHOT"
scalaVersion := "2.11.8"
val orientDBVersion = "3.2.0-SNAPSHOT"

fork := true // if OrientDb version > 2.1-RC5

resolvers ++= Seq(
  Resolver.mavenLocal,
  "Orient Technologies Maven2 Repository" at "http://www.orientechnologies.com/listing/m2",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % Test,
  "com.orientechnologies" % "orientdb-server" % orientDBVersion,
  "com.orientechnologies" % "orientdb-client" % orientDBVersion,
  "com.michaelpollmeier" %% "gremlin-scala" % "3.2.3.1",
  "com.orientechnologies" % "orientdb-gremlin" % orientDBVersion
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
