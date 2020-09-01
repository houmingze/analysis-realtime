package com.atguigu.analysis.server.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

object OffsetManager {


    def getOffset(topic:String,groupId:String): Map[TopicPartition,Long]={
        val jedis: Jedis = RedisUtil.getJedisClient
        val offsetKey = topic + groupId
        val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
        jedis.close()
        if(offsetMap !=null && offsetMap.size()>0){
            javaMap2ScalaMap(topic,offsetMap)
        }else{
            println("没有获取到偏移量")
            null
        }

    }

    def saveOffset(topic: String, groupId: String, ranges: Array[OffsetRange]): Unit = {
        val jedis: Jedis = RedisUtil.getJedisClient
        val offsetKey = topic + groupId
        val map: util.HashMap[String, String] = new util.HashMap[String, String]()
        if(ranges!=null&&ranges.size>0){
            for(range <- ranges){
                println(s"保存分区：${range.partition}，偏移量：${range.untilOffset}")
                map.put(range.partition.toString,range.untilOffset.toString)
            }
            jedis.hmset(offsetKey,map)
        }
    }

    def javaMap2ScalaMap(topic:String,offsetMap: util.Map[String, String]): Map[TopicPartition,Long]={
        var result = mutable.Map[TopicPartition, Long]()
        offsetMap.forEach{
            case(partition,offset)=>{
                println(s"获取分区：${partition}，偏移量：${offset}")
                result.put(new TopicPartition(topic,partition.toInt),offset.toLong)
            }
        }
        result.toMap
    }

}
