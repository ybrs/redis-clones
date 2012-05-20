package org.jcheng
import com.mongodb.casbah.MongoConnection
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.commons.conversions._

import com.mongodb.casbah.Imports._

/**
 * @author jcheng
 *
 */
object CasbahDemo {
  
  def main(argv: Array[String]) {
    val testData = MongoConnection()("jctest")("testdata")
    
    println("Assuming this is not the first run...")
    println("Size should be 2: " + testData.size)
    testData.remove(MongoDBObject("name" -> "firs"))
    println("Size should be 1: " + testData.size)
    testData.remove(MongoDBObject()) // remove everything
    
    testData += MongoDBObject("name" -> "first",  "val"-> 1)
    testData += MongoDBObject("name" -> "second", "val"-> 2)
    
    val objA = MongoDBObject("name" -> "third", "val" -> 3)
    println("Should be 'third': " + objA.getAs[String]("name"))
    
    testData += objA
    
    testData.findOne(MongoDBObject("name" -> "third"), MongoDBObject("val"->1)).foreach { x =>
      println(x)
    }
    
    testData += MongoDBObject("name" -> "four", "val" -> 4, "email" -> "foo@example.com")
    
    println( testData.findOne("email" $exists true).get )
    testData.update(MongoDBObject("name" -> "four"), $set ("email" -> "bar@example.com"))
    println( testData.findOne("email" $exists true).get )
    
    
    
  }

}
