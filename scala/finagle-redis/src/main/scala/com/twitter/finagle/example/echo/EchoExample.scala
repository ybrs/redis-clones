package com.twitter.finagle.example.echo

import java.net.InetSocketAddress

import com.twitter.finagle.Codec
import com.twitter.finagle.CodecFactory
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.builder.Server
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.util.Future
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.Channels
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder
import org.jboss.netty.handler.codec.frame.Delimiters
import org.jboss.netty.handler.codec.string.StringDecoder
import org.jboss.netty.handler.codec.string.StringEncoder
import org.jboss.netty.util.CharsetUtil

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import scala.collection.immutable.WrappedString

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.protocol.{Command, Reply}
import com.twitter.finagle.redis.{Client, Redis}
import com.twitter.finagle.stats.SummarizingStatsReceiver

object EchoServer {
  def main(args: Array[String]) {
    
    /**
     * A very simple service that simply echos its request back
     * as a response. Note that it returns a Future, since everything
     * in Finagle is asynchronous.
     */
    class EchoService extends Service[Command, Reply] {

      var count = 0
      def apply(cmd: Command) = {
          // println(">>>", cmd)
          count += 1
          // println(">>> buffer: >>>", count)
          Future(StatusReply("OK"))
      }

    }

    val stats = new SummarizingStatsReceiver

    // Bind the service to port 6380
    val server: Server = ServerBuilder()
      .codec(Redis())
      .bindTo(new InetSocketAddress(6380))
      .name("echoserver")
      .build(new EchoService)
  }
}


