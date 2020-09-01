package com.atguigu.analysis.server.dwd

import java.util.{Calendar, Date}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.analysis.server.bean.OrderInfo
import com.atguigu.analysis.server.util.{DateTimeUtil, MyKafkaUtils, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DwdOrderInfoApp {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("dwd_order_info_app").setMaster("local[4]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topic : String = "ODS_T_ORDER_INFO"
        val groupId : String = "dwd_order_info_group"

        val offsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if(offsetMap==null){
            inputDStream = MyKafkaUtils.getKafkaStream(topic, ssc, groupId)
        }else{
            inputDStream = MyKafkaUtils.getKafkaStream(topic, ssc, offsetMap, groupId)
        }

        var ranges: Array[OffsetRange] = null
        val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
            rdd => {
                ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }

        val orderInfoDStream: DStream[OrderInfo] = offsetDStream.map {
            record => {
                val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
                val timeStr: String = orderInfo.create_time
                val timeArray: Array[String] = timeStr.split(" ")
                orderInfo.create_date = timeArray(0)
                orderInfo.create_hour = timeArray(1).substring(0,2)
                orderInfo
            }
        }

        val orderInfoWithProvinceDStream: DStream[OrderInfo] = orderInfoDStream.transform {
            rdd => {
                val provinceJsonObjs: List[JSONObject] = PhoenixUtil.queryList("select * from gmall_province_info")
                val provinceInfoMap: Map[String, JSONObject] = provinceJsonObjs.map(obj => (obj.getString("ID"), obj)).toMap
                val provinceBC: Broadcast[Map[String, JSONObject]] = ssc.sparkContext.broadcast(provinceInfoMap)
                val orderRDD: RDD[OrderInfo] = rdd.map {
                    orderInfo => {
                        val provinceMap: Map[String, JSONObject] = provinceBC.value
                        val provinceObj: JSONObject = provinceMap.getOrElse(orderInfo.province_id.toString, null)
                        if (provinceObj != null) {
                            orderInfo.province_name = provinceObj.getString("NAME")
                            orderInfo.province_area_code = provinceObj.getString("AREA_CODE")
                            orderInfo.province_3166_2_code = provinceObj.getString("ISO_3166_2")
                        }
                        orderInfo
                    }
                }
                orderRDD
            }
        }
        orderInfoWithProvinceDStream.print(100)

        ssc.start()
        ssc.awaitTermination()

    }

}
