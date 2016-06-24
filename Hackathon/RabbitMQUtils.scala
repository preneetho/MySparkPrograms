package com.rabbitMQ.receiver

import scala.collection.JavaConverters._

import scala.reflect.ClassTag

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object RabbitMQUtils {

  /**
   * Create an input stream that receives messages from a RabbitMQ.
   * @param ssc          StreamingContext object
   * @param params       RabbitMQ params
   */
  def createStream(ssc: StreamingContext, params: Map[String, String]): ReceiverInputDStream[String] = {
    new RabbitMQInputDStream(ssc, params)
  }

  /**
   * Create an input stream that receives messages from a RabbitMQ queue.
   * @param jssc         JavaStreamingContext object
   * @param params       RabbitMQ params
   */
  def createJavaStream(jssc: JavaStreamingContext,
                       params: java.util.Map[String, String]): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, params.asScala.toMap)
  }

}