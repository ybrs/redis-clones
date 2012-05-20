package org.jcheng.finagle

import sbt._
import Keys._

/**
 * @author jcheng
 *
 */
object MainBuild extends Build {

  val project = Project(id="finagle", base=file(".")) settings (
    name := "Scala Example",
    version := "1.0.1",
    organization  := "org.jcheng.finagle",
    scalaVersion  := "2.8.1", // MongoDB repository only supports Scala 2.8.1
    
    javaOptions in run += "-Xmx512m -Xconcgc -verbosegc", 
    
    resolvers ++= Seq(
        "twitter.com" at "http://maven.twttr.com/",
        "repo.codahale.com" at "http://repo.codahale.com"
    ),
    libraryDependencies ++= Seq(
    	// Finagle
        "com.twitter" % "finagle-core" % "1.9.4",
        "com.twitter" % "finagle-http" % "1.9.4",
        "com.twitter" % "naggati" % "2.2.0",
        
        // JSON
        "org.codehaus.jackson" % "jackson-core-asl"  % "1.8.1",
        "org.codehaus.jackson" % "jackson-mapper-asl" % "1.8.1",
        "com.codahale" % "jerkson_2.8.1" % "0.1.4",
        
        // MongoDB
        "com.mongodb.casbah" % "casbah_2.8.1" % "2.1.5.0",
        
        // Logging
        "org.slf4j" % "slf4j-simple" % "1.6.3",
        "ch.qos.logback" % "logback-core" % "0.9.30",
        
        // Testing
        "org.apache.httpcomponents" % "httpclient" % "4.1.2",
        "org.scalatest" % "scalatest_2.8.1" % "1.5.1" % "test"
    )
  )
}
