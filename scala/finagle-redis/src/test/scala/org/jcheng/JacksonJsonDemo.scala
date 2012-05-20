package org.jcheng
import com.codahale.jerkson.Json.generate
import java.util.Date
import scala.collection.mutable.Buffer
import scala.collection.mutable.ListBuffer

/**
 * @author jcheng
 *
 */
object JacksonJsonDemo {

  def main(argv: Array[String]) {
    val bar = new Bar(age=10)
    bar.name = "Bar"
    println(generate(bar))
  }

}

case class Foo(var name:String = "Foo")
case class Bar(var age:Int = 0) extends Foo


class DVD {
  
  var _name:String = ""
  
  def name = _name
  
  def name_= (newVal:String) = _name = newVal
  
  
}

case class Book(var title: String = null,
  var categories: Buffer[String] = ListBuffer(),
  var pageCount: Int = 0,
  var censored: Boolean = false,
  var createDate: Date = new Date(),
  var props: Map[String, String] = Map()) {

}
case class Digital(var url: String) extends Book
case class Paper(var pctRecycled: Float) extends Book