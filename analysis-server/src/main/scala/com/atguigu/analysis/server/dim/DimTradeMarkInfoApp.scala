package com.atguigu.analysis.server.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.analysis.server.bean.{ProvinceInfo, TrademarkInfo}
import com.atguigu.analysis.server.util.{MyKafkaUtils, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DimTradeMarkInfoApp {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("dim_trademark_info_app").setMaster("local[4]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topic:String = "ODS_T_BASE_TRADEMARK"

        val groupId :String ="dim_trademark_info_group"

        val offsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if(offsetMap == null){
            inputDStream = MyKafkaUtils.getKafkaStream(topic, ssc, groupId)
        }else{
            inputDStream = MyKafkaUtils.getKafkaStream(topic,ssc,offsetMap,groupId)
        }

        var  ranges: Array[OffsetRange] = null
        val transformDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform { rdd =>
            ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }

        val jsonObjDSream: DStream[JSONObject] = transformDStream.map { record =>
            JSON.parseObject(record.value())
        }

       jsonObjDSream.foreachRDD {
            rdd=>{
                val trademarkRDD: RDD[TrademarkInfo] = rdd.map {
                    jsonObj => {
                        val trademark: TrademarkInfo = TrademarkInfo(
                            jsonObj.getString("tm_id"),
                            jsonObj.getString("tm_name")
                        )
                        trademark
                    }
                }

                trademarkRDD.saveToPhoenix("GMALL_TRADEMARK_INFO",
                    Seq("TM_ID","TM_NAME"),new Configuration,
                    Some("hadoop102,hadoop103,hadoop104:2181")
                )

                OffsetManager.saveOffset(topic,groupId,ranges)

            }
        }

        ssc.start()
        ssc.awaitTermination()

    }

}
