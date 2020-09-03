package com.atguigu.analysis.server.dws

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.analysis.server.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.analysis.server.util.{MyKafkaUtils, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DwsOrderWideCacheApp {

    private val serializeConfig: SerializeConfig = new SerializeConfig(true)

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("dws_order_wide_app").setMaster("local[4]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topicOrderInfo = "DWD_ORDER_INFO"
        val topicOrderDetail = "DWD_ORDER_DETAIL"
        val groupId = "dws_order_wide_group"

        val orderInfoOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderInfo, groupId)
        val orderDetailOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderDetail, groupId)

        var orderInfoInputDStream: InputDStream[ConsumerRecord[String, String]] = null
        var orderDetailInputDStream: InputDStream[ConsumerRecord[String, String]] = null

        if (orderInfoOffsetMap == null) {
            orderInfoInputDStream = MyKafkaUtils.getKafkaStream(topicOrderInfo, ssc, groupId)
        } else {
            orderInfoInputDStream = MyKafkaUtils.getKafkaStream(topicOrderInfo, ssc, orderInfoOffsetMap, groupId)
        }
        if (orderDetailOffsetMap == null) {
            orderDetailInputDStream = MyKafkaUtils.getKafkaStream(topicOrderDetail, ssc, groupId)
        } else {
            orderDetailInputDStream = MyKafkaUtils.getKafkaStream(topicOrderDetail, ssc, orderDetailOffsetMap, groupId)
        }

        var orderInfoRanges: Array[OffsetRange] = null
        var orderDetailRanges: Array[OffsetRange] = null

        val orderInfoDStream: DStream[ConsumerRecord[String, String]] = orderInfoInputDStream.transform {
            rdd => {
                orderInfoRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }
        val orderDetailDStream: DStream[ConsumerRecord[String, String]] = orderDetailInputDStream.transform {
            rdd => {
                orderDetailRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }

        val orderInfoDS: DStream[OrderInfo] = orderInfoDStream.map {
            rdd => {
                val orderInfo: OrderInfo = JSON.parseObject(rdd.value(), classOf[OrderInfo])
                orderInfo
            }
        }
        val orderDetailDS: DStream[OrderDetail] = orderDetailDStream.map {
            rdd => {
                val orderDetail: OrderDetail = JSON.parseObject(rdd.value(), classOf[OrderDetail])
                orderDetail
            }
        }

        val orderInfoWithKeyDS: DStream[(Long, OrderInfo)] = orderInfoDS.map { orderInfo => (orderInfo.id, orderInfo) }
        val orderDetailWithKeyDS: DStream[(Long, OrderDetail)] = orderDetailDS.map { orderDetail => (orderDetail.order_id, orderDetail) }

        val joinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDS.fullOuterJoin(orderDetailWithKeyDS)

        val orderWideDStream: DStream[OrderWide] = joinDStream.mapPartitions { it =>
            var orderWides = new ListBuffer[OrderWide]
            val jedis: Jedis = RedisUtil.getJedisClient
            val optOrderList: List[(Long, (Option[OrderInfo], Option[OrderDetail]))] = it.toList
            optOrderList.foreach {
                case (orderId, (orderInfoOpt, orderDetailOpt)) => {
                    if (orderInfoOpt != null && orderDetailOpt != null) {
                        val orderInfo: OrderInfo = orderInfoOpt.get
                        val orderDetail: OrderDetail = orderDetailOpt.get
                        orderWides += new OrderWide(orderInfo,orderDetail)

                        val orderInfoKey = "orderjoin:orderInfo" + orderId
                        jedis.setex(orderInfoKey,600,JSON.toJSONString(orderInfo,serializeConfig))
                    }
                    if (orderInfoOpt == null && orderDetailOpt != null) {
                        val orderDetail: OrderDetail = orderDetailOpt.get
                        val orderDetailKey = "orderjoin:orderDetail:"+ orderId
                        val orderInfoKey = "orderjoin:orderInfo:" + orderId

                        val orderInfoStr: String = jedis.get(orderInfoKey)
                        if(orderInfoStr != null && orderInfoStr.size >0){
                            val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
                            orderWides += new OrderWide(orderInfo,orderDetail)
                        }else{
                            jedis.sadd(orderDetailKey,JSON.toJSONString(orderDetail,serializeConfig))
                            jedis.expire(orderDetailKey,600)
                        }
                    }
                    if (orderInfoOpt != null && orderDetailOpt == null) {
                        val orderInfo: OrderInfo = orderInfoOpt.get
                        val orderDetailKey = "orderjoin:orderDetail:"+ orderId
                        val orderInfoKey = "orderjoin:orderInfo:" + orderId
                        jedis.setex(orderInfoKey,600,JSON.toJSONString(orderInfo,serializeConfig))
                        val orderDetailStrSet: util.Set[String] = jedis.smembers(orderDetailKey)
                        if(orderDetailStrSet != null && orderDetailStrSet.size() > 0){
                            orderDetailStrSet.forEach{orderDetailStr=>
                                val orderDetail: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
                                orderWides += new OrderWide(orderInfo,orderDetail)
                            }
                        }
                    }
                }
            }
            jedis.close()
            orderWides.toIterator
        }

        orderWideDStream.print(1000)
        OffsetManager.saveOffset(topicOrderInfo,groupId,orderInfoRanges)
        OffsetManager.saveOffset(topicOrderDetail,groupId,orderDetailRanges)

        ssc.start()
        ssc.awaitTermination()

    }

}
