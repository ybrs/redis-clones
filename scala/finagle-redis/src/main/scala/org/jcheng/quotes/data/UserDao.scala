package org.jcheng.quotes.data

/**
 * @author jcheng
 *
 */
trait UserDao {

  def getUser(email:String): User
}