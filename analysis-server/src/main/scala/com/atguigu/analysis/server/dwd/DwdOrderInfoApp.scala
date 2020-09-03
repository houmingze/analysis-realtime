package com.atguigu.analysis.server.dwd

import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.analysis.server.bean.{OrderInfo, UserInfo}
import com.atguigu.analysis.server.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object DwdOrderInfoApp {

    private val serializeConfig: SerializeConfig = new SerializeConfig(true)

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

        orderInfoWithProvinceDStream.foreachRDD{
            rdd=>{
                rdd.foreachPartition{
                    it=>{
                        val orderInfoList: List[OrderInfo] = it.toList
                        var userIds = mutable.Set[Long]()
                        orderInfoList.foreach{ orderInfo=>
                            userIds.add(orderInfo.user_id)
                        }
                        var userMap: Map[String, UserInfo] = null
                        if(userIds != null && userIds.size >0 ){
                            val userIdStr: String = userIds.map(id=>s"'${id}'").mkString(",")
                            val querySql = s"select * from gmall_user_info where id in (${userIdStr})"
                            val objects: List[JSONObject] = PhoenixUtil.queryList(querySql)
                            if(objects != null && objects.size > 0){
                                userMap = objects.map(obj => (obj.getString("ID"), JSON.parseObject(obj.toJSONString, classOf[UserInfo]))).toMap
                            }
                        }
                        orderInfoList.foreach{ orderInfo=>
                            if(userMap != null && userMap.size >0 ){
                                val userInfo: UserInfo = userMap.getOrElse(orderInfo.user_id.toString,null)
                                var user_age_group:String = null
                                var user_gender:String = null
                                if(userInfo != null){
                                    userInfo.gender match {
                                        case "F" => user_gender = "女"
                                        case "M" => user_gender = "男"
                                        case _ =>
                                    }
                                    if(userInfo.birthday != null){
                                        val year: Int = userInfo.birthday.substring(0, 4).toInt
                                        val age: Int = DateTimeUtil.getAge(year)
                                        user_age_group = DateTimeUtil.getAgeGroup(age)
                                    }
                                }
                                orderInfo.user_age_group = user_age_group
                                orderInfo.user_gender = user_gender
                            }
                            val topic = "DWD_ORDER_INFO"
                            MyKafkaSender.send(topic,JSON.toJSONString(orderInfo,serializeConfig))
                        }
                        val dateStr: String = DateTimeUtil.newFormat.format(new Date())
                        val orders: List[(OrderInfo, String)] = orderInfoList.map(order => (order, order.id.toString))
                        MyEsUtil.saveDocBulk(orders,"gmall_order_info"+dateStr)
                    }
                }
                OffsetManager.saveOffset(topic,groupId,ranges)
            }
        }

        ssc.start()
        ssc.awaitTermination()

    }

}
