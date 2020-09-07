package com.atguigu.analysis.server.dwd

import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.analysis.server.bean.{OrderDetail, OrderInfo, UserInfo}
import com.atguigu.analysis.server.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object DwdOrderDetailApp {

    private val serializeConfig: SerializeConfig = new SerializeConfig(true)

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("dwd_order_detail_app").setMaster("local[4]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topic: String = "ODS_T_ORDER_DETAIL"
        val groupId: String = "dwd_order_detail_group"

        val offsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetMap == null) {
            inputDStream = MyKafkaUtils.getKafkaStream(topic, ssc, groupId)
        } else {
            inputDStream = MyKafkaUtils.getKafkaStream(topic, ssc, offsetMap, groupId)
        }

        var ranges: Array[OffsetRange] = null
        val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
            rdd => {
                ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }

        val orderDetailDStream: DStream[OrderDetail] = offsetDStream.map {
            record => {
                val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
                orderDetail
            }
        }
        val skuInfoJsonObjs: List[JSONObject] = PhoenixUtil.queryList("select * from gmall_sku_info")
        val skuInfoMap: Map[String, JSONObject] = skuInfoJsonObjs.map(obj => (obj.getString("ID"), obj)).toMap

        val trademarkJsonObjs: List[JSONObject] = PhoenixUtil.queryList("select * from gmall_trademark_info")
        val trademarkMap: Map[String, JSONObject] = trademarkJsonObjs.map(obj => (obj.getString("TM_ID"), obj)).toMap

        val category3JsonObjs: List[JSONObject] = PhoenixUtil.queryList("select * from gmall_category3_info")
        val category3Map: Map[String, JSONObject] = category3JsonObjs.map(obj => (obj.getString("ID"), obj)).toMap

        val spuInfoJsonObjs: List[JSONObject] = PhoenixUtil.queryList("select * from gmall_spu_info")
        val spuInfoMap: Map[String, JSONObject] = spuInfoJsonObjs.map(obj => (obj.getString("ID"), obj)).toMap

        var bcMap = mutable.Map[String, Map[String, JSONObject]]()
        bcMap.put("skuInfo", skuInfoMap)
        bcMap.put("trademark", trademarkMap)
        bcMap.put("category3", category3Map)
        bcMap.put("spuInfo", spuInfoMap)

        val dimBC: Broadcast[mutable.Map[String, Map[String, JSONObject]]] = ssc.sparkContext.broadcast(bcMap)

        orderDetailDStream.foreachRDD {
            rdd => {
                rdd.foreachPartition {
                    it => {
                        val dimMap: mutable.Map[String, Map[String, JSONObject]] = dimBC.value
                        val skuInfoMap: Option[Map[String, JSONObject]] = dimMap.get("skuInfo")
                        val trademarkMap: Option[Map[String, JSONObject]] = dimMap.get("trademark")
                        val category3Map: Option[Map[String, JSONObject]] = dimMap.get("category3")
                        val spuInfoMap: Option[Map[String, JSONObject]] = dimMap.get("spuInfo")

                        val orderDetails: List[OrderDetail] = it.toList
                        orderDetails.foreach {
                            orderDetail => {
                                val skuInfoJsonObj: JSONObject = skuInfoMap.get(orderDetail.sku_id.toString)
                                orderDetail.spu_id = skuInfoJsonObj.getLong("SPU_ID")
                                orderDetail.tm_id = skuInfoJsonObj.getLong("TM_ID")
                                orderDetail.category3_id = skuInfoJsonObj.getLong("CATEGORY3_ID")
                                val spuInfoJsonObj: JSONObject = spuInfoMap.get(orderDetail.spu_id.toString)
                                val trademarkJsonObj: JSONObject = trademarkMap.get(orderDetail.tm_id.toString)
                                val category3JsonObj: JSONObject = category3Map.get(orderDetail.category3_id.toString)
                                orderDetail.tm_name = trademarkJsonObj.getString("TM_NAME")
                                orderDetail.category3_name = category3JsonObj.getString("NAME")
                                orderDetail.spu_name = spuInfoJsonObj.getString("SPU_NAME")

                                val topic = "DWD_ORDER_DETAIL"
                                MyKafkaSender.send(topic, orderDetail.order_id.toString, JSON.toJSONString(orderDetail, serializeConfig))

                            }
                        }
                        //val dateStr: String = DateTimeUtil.newFormat.format(new Date())
                        //val orders: List[(OrderDetail, String)] = orderDetails.map(order => (order, order.id.toString))
                        //MyEsUtil.saveDocBulk(orders, "gmall_order_detail" + dateStr)
                    }
                }
                OffsetManager.saveOffset(topic, groupId, ranges)
            }
        }

        ssc.start()
        ssc.awaitTermination()

    }

}
