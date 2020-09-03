package com.atguigu.analysis.server.dim

import com.alibaba.fastjson.JSON
import com.atguigu.analysis.server.bean.UserInfo
import com.atguigu.analysis.server.util.{MyKafkaUtils, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DimUserInfoApp {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dim_user_info_app")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topic = "ODS_T_USER_INFO"
        val groupId = "dim_user_info_group"

        val offsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
        var inputDStream : InputDStream[ConsumerRecord[String, String]] = null
        if(offsetMap == null){
            inputDStream = MyKafkaUtils.getKafkaStream(topic, ssc, groupId)
        }else{
            inputDStream = MyKafkaUtils.getKafkaStream(topic,ssc,offsetMap,groupId)
        }

        var ranges: Array[OffsetRange] = null

        val transformDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
            rdd => {
                ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }

        val userInfoDStream: DStream[UserInfo] = transformDStream.map {
            record => {
                val dataObjStr: String = record.value()
                val userInfo: UserInfo = JSON.parseObject(dataObjStr, classOf[UserInfo])
                userInfo
            }
        }

        userInfoDStream.foreachRDD{
            rdd=>{
                rdd.saveToPhoenix("GMALL_USER_INFO",
                    Seq("ID","LOGIN_NAME","NICK_NAME","NAME","PHONE_NUM",
                        "EMAIL","USER_LEVEL","BIRTHDAY","GENDER","CREATE_TIME","OPERATE_TIME"),
                    new Configuration,
                    Some("hadoop102,hadoop103,hadoop104:2181")
                )

                OffsetManager.saveOffset(topic,groupId,ranges)
            }
        }

        ssc.start()
        ssc.awaitTermination()

    }

}
