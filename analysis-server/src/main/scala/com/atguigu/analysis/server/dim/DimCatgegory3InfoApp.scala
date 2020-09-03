package com.atguigu.analysis.server.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.analysis.server.bean.{Category3Info, TrademarkInfo}
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

object DimCatgegory3InfoApp {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("dim_category3_info_app").setMaster("local[4]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topic:String = "ODS_T_BASE_CATEGORY3"

        val groupId :String ="dim_category3_info_group"

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
                val category3RDD: RDD[Category3Info] = rdd.map {
                    jsonObj => {
                        val category3: Category3Info = Category3Info(
                            jsonObj.getString("id"),
                            jsonObj.getString("name"),
                            jsonObj.getString("category2_id")
                        )
                        category3
                    }
                }

                category3RDD.saveToPhoenix("GMALL_CATEGORY3_INFO",
                    Seq("ID","NAME","CATEGORY2_ID"),new Configuration,
                    Some("hadoop102,hadoop103,hadoop104:2181")
                )

                OffsetManager.saveOffset(topic,groupId,ranges)

            }
        }

        ssc.start()
        ssc.awaitTermination()

    }

}
