val ScalatraVersion = "2.6.3"

organization := "com.amazon.deequ"

name := "backend"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.7"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.scalatra" %% "scalatra" % ScalatraVersion,
  "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test",
  "org.scalatra" %% "scalatra-scalate" % ScalatraVersion,
  "org.scalatra" %% "scalatra-json" % "2.6.3",
  "org.scalatra" %% "scalatra-swagger"  % "2.6.3",
  "org.json4s"   %% "json4s-jackson" % "3.6.2",
  "org.json4s"   %% "json4s-native" % "3.6.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "runtime" exclude("org.slf4j", "slf4j-log4j12"),
  "org.eclipse.jetty" % "jetty-webapp" % "9.2.15.v20160210" % "container;provided",
  "org.eclipse.jetty" % "jetty-server" % "9.2.15.v20160210" % "container;provided",
  "org.eclipse.jetty" % "jetty-servlet" % "9.2.15.v20160210" % "container;provided",
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
  "com.sun.jersey" % "jersey-server" % "1.19.4",
  "org.mariadb.jdbc" % "mariadb-java-client" % "2.3.0",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.0.0.jre8"
)

unmanagedBase := baseDirectory.value / "lib"
enablePlugins(SbtTwirl)
enablePlugins(ScalatraPlugin)
