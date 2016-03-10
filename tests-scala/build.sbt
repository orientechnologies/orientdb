name := "orientdb-tp3-test"
organization := "com.michaelpollmeier"
version := "1.0.0-SNAPSHOT"
scalaVersion := "2.11.7"
val orientDBVersion = "2.1.12"

fork := true // if OrientDb version > 2.1-RC5

resolvers ++= Seq(
  Resolver.mavenLocal,
  "Orient Technologies Maven2 Repository" at "http://www.orientechnologies.com/listing/m2",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % Test,
  "com.orientechnologies" % "orientdb-graphdb" % orientDBVersion,
  "com.orientechnologies" % "orientdb-client" % orientDBVersion,
  "com.michaelpollmeier" %% "gremlin-scala" % "3.1.1-incubating.0",
  "com.michaelpollmeier" % "orientdb-gremlin" % "3.1.1-incubating.1"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
