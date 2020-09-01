package com.atguigu.analysis.server.app

import java.lang
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.analysis.server.bean.StartLog
import com.atguigu.analysis.server.common.Constant
import com.atguigu.analysis.server.util.{DateTimeUtil, MyEsUtil, MyKafkaUtils, OffsetManager, RedisUtil}
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
    val UID = "uid"
    val AR = "ar"
    val CH = "ch"
    val VC = "vc"
    val DAY_STR = "dayStr";
    val HOUT_STR = "hourStr";

    //{"common":{"ar":"440000","ba":"iPhone","ch":"Appstore","md":"iPhone Xs Max","mid":"mid_70","os":"iOS 13.2.9","uid":"371","vc":"v2.1.134"},
    // "start":{"entry":"icon","loading_time":4511,"open_ad_id":2,"open_ad_ms":6926,"open_ad_skip_ms":6731},"ts":1598351203000}
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("StartCount-App")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        val offsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(Constant.KAFKA_TOPIC_GMALL_START, Constant.GROUP_CONSUMER_START_COUNT)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetMap != null) {
            inputDStream = MyKafkaUtils.getKafkaStream(Constant.KAFKA_TOPIC_GMALL_START, ssc, offsetMap, Constant.GROUP_CONSUMER_START_COUNT)
        } else {
            inputDStream = MyKafkaUtils.getKafkaStream(Constant.KAFKA_TOPIC_GMALL_START, ssc, Constant.GROUP_CONSUMER_START_COUNT)
        }

        var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
        val offsetDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform {
            rdd => {
                ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        }
        val filterDStream: DStream[StartLog] = offsetDStream.mapPartitions {
            it => {
                val cal: Calendar = Calendar.getInstance()
                val jedis: Jedis = RedisUtil.getJedisClient
                val result: ListBuffer[StartLog] = new ListBuffer[StartLog]
                it.foreach {
                    record => {
                        val logJsonObj: JSONObject = JSON.parseObject(record.value())
                        val ts: lang.Long = logJsonObj.getLong(TS)

                        cal.setTimeInMillis(ts)
                        val dayStr: String = DateTimeUtil.getDayStr(cal)
                        val hourStr: String = DateTimeUtil.getHourStr(cal)
                        val commonObj: JSONObject = logJsonObj.getJSONObject(COMMON)
                        val mid: String = commonObj.getString(MID)
                        val key: String = Constant.KEY_PRE_START_COUNT + dayStr + mid
                        val str: String = jedis.getSet(key, Constant.VALUE_START_COUNT)
                        if (str == null) {
                            val uid: String = commonObj.getString(UID)
                            val ar: String = commonObj.getString(AR)
                            val ch: String = commonObj.getString(CH)
                            val vc: String = commonObj.getString(VC)
                            val startLog: StartLog = StartLog(mid, uid, ar, ch, vc, dayStr, hourStr, DateTimeUtil.getMiStr(cal), ts)
                            result.append(startLog)
                        }
                    }
                }
                jedis.close()
                result.toIterator
            }
        }
        filterDStream.foreachRDD {
            rdd => {
                val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
                rdd.foreachPartition {
                    it => {
                        val startLogWithIdList: List[(StartLog, String)] = it.toList.map(startLog=>(startLog,startLog.mid))
                        MyEsUtil.saveDocBulk(startLogWithIdList,"gmall_dau_info_"+dateStr)
                    }
                }
                OffsetManager.saveOffset(Constant.KAFKA_TOPIC_GMALL_START, Constant.GROUP_CONSUMER_START_COUNT, ranges)
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }

}
