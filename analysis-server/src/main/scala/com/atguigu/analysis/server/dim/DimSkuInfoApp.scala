package com.atguigu.analysis.server.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.analysis.server.bean.{SkuInfo, SpuInfo}
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

object DimSkuInfoApp {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("dim_sku_info_app").setMaster("local[4]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topic:String = "ODS_T_SKU_INFO"

        val groupId :String ="dim_sku_info_group"

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
                val skuInfoRDD: RDD[SkuInfo] = rdd.map {
                    jsonObj => {
                        val skuInfo: SkuInfo = SkuInfo(
                            jsonObj.getString("id"),
                            jsonObj.getString("spu_id"),
                            jsonObj.getString("price"),
                            jsonObj.getString("sku_name"),
                            jsonObj.getString("sku_desc"),
                            jsonObj.getString("weight"),
                            jsonObj.getString("tm_id"),
                            jsonObj.getString("category3_id"),
                            jsonObj.getString("sku_default_img"),
                            jsonObj.getString("create_time")
                        )
                        skuInfo
                    }
                }
                skuInfoRDD.saveToPhoenix("GMALL_SKU_INFO",
                    Seq("ID","SPU_ID","PRICE","SKU_NAME","SKU_DESC","WEIGHT","TM_ID","CATEGORY3_ID","SKU_DEFAULT_IMG","CREATE_TIME"),
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
