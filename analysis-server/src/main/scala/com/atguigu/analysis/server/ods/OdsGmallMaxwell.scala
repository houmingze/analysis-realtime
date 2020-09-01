package com.atguigu.analysis.server.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.analysis.server.util.{MyKafkaSender, MyKafkaUtils, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OdsGmallMaxwell {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("ods_gmall_maxwell_app").setMaster("local[4]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val topic  = "GMALL_DB_MAXWELL"
        val groupId = "ods_gmall_maxwell_group"

        val offsets: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if(offsets ==null){
            inputDStream = MyKafkaUtils.getKafkaStream(topic, ssc, groupId)
        }else{
            inputDStream = MyKafkaUtils.getKafkaStream(topic, ssc, offsets, groupId)
        }

        var ranges: Array[OffsetRange] = null;
        val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.map {
            rdd => {
                ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }

        val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
            record => {
                val str: String = record.value()
                JSON.parseObject(str)
            }
        }

        jsonObjDStream.foreachRDD{rdd=>
            rdd.foreach{
                jsonObj=>{
                    val dataString: String = jsonObj.getString("data")
                    val table: String = jsonObj.getString("table")
                    val topic = "ODS_T" + table
                    MyKafkaSender.send(topic,dataString)
                }
            }
            OffsetManager.saveOffset(topic,groupId,ranges)
        }


        ssc.start()
        ssc.awaitTermination()
    }

}
