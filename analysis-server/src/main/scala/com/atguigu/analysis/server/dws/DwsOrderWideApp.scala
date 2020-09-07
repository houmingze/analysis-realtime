package com.atguigu.analysis.server.dws

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.analysis.server.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.analysis.server.util.{MyKafkaUtils, OffsetManager, RedisUtil}
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object DwsOrderWideApp {

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

        /*val orderInfoWithKeyDS: DStream[(Long, OrderInfo)] = orderInfoDS.map { orderInfo => (orderInfo.id, orderInfo) }
        val orderDetailWithKeyDS: DStream[(Long, OrderDetail)] = orderDetailDS.map { orderDetail => (orderDetail.order_id, orderDetail) }
        orderInfoWithKeyDS.join(orderDetailWithKeyDS).print(1000)*/

        val orderInfoWindowDS: DStream[OrderInfo] = orderInfoDS.window(Seconds(15), Seconds(5))
        val orderDetailWindowDS: DStream[OrderDetail] = orderDetailDS.window(Seconds(15), Seconds(5))
        val orderInfoWithKeyDS: DStream[(Long, OrderInfo)] = orderInfoWindowDS.map { orderInfo => (orderInfo.id, orderInfo) }
        val orderDetailWithKeyDS: DStream[(Long, OrderDetail)] = orderDetailWindowDS.map { orderDetail => (orderDetail.order_id, orderDetail) }

        val joinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDS.join(orderDetailWithKeyDS)
        val orderWideDStream: DStream[OrderWide] = joinDStream.map { case (orderId, (orderInfo, orderDetail)) => new OrderWide(orderInfo, orderDetail) }

        val filterDStream: DStream[OrderWide] = orderWideDStream.mapPartitions { orderWideIt =>
            val jedis: Jedis = RedisUtil.getJedisClient
            val orderWides: ListBuffer[OrderWide] = new ListBuffer[OrderWide]
            val orderWideList: List[OrderWide] = orderWideIt.toList
            if (orderWideList != null && orderWideList.size > 0) {
                orderWideList.foreach { orderWide =>
                    val key = "orderwide:" + orderWide.order_id + "_" + orderWide.order_detail_id
                    val result: String = jedis.getSet(key, "0")
                    if (result == null) {
                        calOrderDetailFinalAmount(orderWide, jedis)
                        orderWides.append(orderWide)
                    }
                }
            }
            jedis.close()
            orderWides.toIterator
        }

        filterDStream.cache()
        filterDStream.print(1000)

        val sparkSession: SparkSession = SparkSession.builder().appName("order_wide_spark_app").getOrCreate()
        import sparkSession.implicits._

        filterDStream.foreachRDD{rdd=>
            var prop= new Properties()
            prop.setProperty("user","default")
            prop.setProperty("password","zaijian")
            prop.setProperty("driver", "ru.yandex.clickhouse.ClickHouseDriver")
            val df: DataFrame = rdd.toDF()
            df.write.mode(SaveMode.Append)
                    .option("batchsize", "100")
                    .option("isolationLevel", "NONE") // 设置事务
                    .option("numPartitions", "4") // 设置并发
                    .jdbc("jdbc:clickhouse://hadoop102:8123/gmall", "order_wide", prop)
        }

        OffsetManager.saveOffset(topicOrderInfo, groupId, orderInfoRanges)
        OffsetManager.saveOffset(topicOrderDetail, groupId, orderDetailRanges)

        ssc.start()
        ssc.awaitTermination()

    }

    def calOrderDetailFinalAmount(orderWide: OrderWide, jedis: Jedis): Unit = {
        var tmpOriginalAmount = BigDecimal(0)
        var tmpFinalAmount = BigDecimal(0)
        var finalDetailAmount = BigDecimal(0)
        val originalDetailAmount = BigDecimal(orderWide.sku_price) * BigDecimal(orderWide.sku_num)
        val originalTotalAmountKey = "originalAmount:" + orderWide.order_id
        val detailMap: util.Map[String, String] = jedis.hgetAll(originalTotalAmountKey)
        if (!MapUtils.isEmpty(detailMap)) {
            detailMap.forEach{ case (orderDetailId,orderDetailOriginalPrice)=>
                if(orderWide.order_detail_id.toString != orderDetailId){
                    tmpOriginalAmount += BigDecimal(orderDetailOriginalPrice)
                    tmpFinalAmount += BigDecimal(orderWide.final_total_amount) * BigDecimal(orderDetailOriginalPrice) / BigDecimal(orderWide.original_total_amount)
                }
            }
        }
        tmpOriginalAmount += originalDetailAmount
        if(orderWide.original_total_amount == tmpOriginalAmount){
            finalDetailAmount = BigDecimal(orderWide.final_total_amount) - tmpFinalAmount
        }else{
            finalDetailAmount = BigDecimal(orderWide.final_total_amount) * originalDetailAmount / BigDecimal(orderWide.original_total_amount)
        }
        orderWide.final_detail_amount = finalDetailAmount.doubleValue()
        jedis.hset(originalTotalAmountKey,orderWide.order_detail_id.toString,originalDetailAmount.toString())
    }

}
