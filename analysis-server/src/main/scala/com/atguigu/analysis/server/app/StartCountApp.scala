package com.atguigu.analysis.server.app

import java.lang
import java.util.Calendar

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.analysis.server.common.Constant
import com.atguigu.analysis.server.util.{DateTimeUtil, MyKafkaUtils, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object StartCountApp {

    val TS = "ts"
    val COMMON = "common"
    val MID = "mid"
    val DAY_STR = "dayStr";
    val HOUT_STR = "hourStr";

    //{"common":{"ar":"440000","ba":"iPhone","ch":"Appstore","md":"iPhone Xs Max","mid":"mid_70","os":"iOS 13.2.9","uid":"371","vc":"v2.1.134"},
    // "start":{"entry":"icon","loading_time":4511,"open_ad_id":2,"open_ad_ms":6926,"open_ad_skip_ms":6731},"ts":1598351203000}
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("StartCount-App")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        val offsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(Constant.KAFKA_TOPIC_GMALL_START, Constant.GROUP_CONSUMER_START_COUNT)
        if(offsetMap!=null){
            inputDStream= MyKafkaUtils.getKafkaStream(Constant.KAFKA_TOPIC_GMALL_START, ssc,offsetMap, Constant.GROUP_CONSUMER_START_COUNT)
        }else{
            inputDStream= MyKafkaUtils.getKafkaStream(Constant.KAFKA_TOPIC_GMALL_START, ssc, Constant.GROUP_CONSUMER_START_COUNT)
        }

        var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
        val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
            rdd =>
                val ranges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
                offsetRanges = ranges.offsetRanges
                rdd
        }

        val filterDStream: DStream[JSONObject] = offsetDStream.mapPartitions {
            it => {
                //val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
                val cal: Calendar = Calendar.getInstance()
                val jedis: Jedis = RedisUtil.getJedisClient
                val result: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
                it.foreach {
                    record => {
                        val logJsonObj: JSONObject = JSON.parseObject(record.value())
                        val ts: lang.Long = logJsonObj.getLong(TS)

                        cal.setTimeInMillis(ts)
                        val dayStr: String = DateTimeUtil.getDayStr(cal)
                        val hourStr: String = DateTimeUtil.getHourStr(cal)
                        //val timeStr: String = format.format(new Date(ts))
                        //val array: Array[String] = timeStr.split(" ")
                        //val dayStr = array(0)
                        //val hourStr = array(1)
                        val mid: String = logJsonObj.getJSONObject(COMMON).getString(MID)
                        val key: String = Constant.KEY_PRE_START_COUNT + dayStr + mid
                        val str: String = jedis.getSet(key, Constant.VALUE_START_COUNT)
                        if (str == null) {
                            logJsonObj.put(DAY_STR, dayStr)
                            logJsonObj.put(HOUT_STR, hourStr)
                            result.append(logJsonObj)
                        }
                    }
                }
                jedis.close()
                result.toIterator
            }
        }
        //filterDStream.map(_.toJSONString).print(100)
        filterDStream.foreachRDD{
            rdd=>{
                OffsetManager.saveOffset(Constant.KAFKA_TOPIC_GMALL_START,Constant.GROUP_CONSUMER_START_COUNT,offsetRanges)
            }
        }

        ssc.start()
        ssc.awaitTermination()


        /* val filteredDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions { jsonObjItr =>
             val jedis: Jedis = RedisUtil.getJedisClient // 1 连接池
             val jsonList: List[JSONObject] = jsonObjItr.toList
             println("过滤前："+jsonList.size )
             val filteredList = new ListBuffer[JSONObject]
             for (jsonObj <- jsonList) {
                 val dauKey = "dau:" + jsonObj.get("dt")
                 val mid = jsonObj.getJSONObject("common").getString("mid")
                 val ifNonExists: lang.Long = jedis.sadd(dauKey, mid)
                 if (ifNonExists == 1) {
                     filteredList += jsonObj
                 }
             }
             jedis.close()
             println("过滤后："+filteredList.size )
             filteredList.toIterator
         }*/
    }

}
