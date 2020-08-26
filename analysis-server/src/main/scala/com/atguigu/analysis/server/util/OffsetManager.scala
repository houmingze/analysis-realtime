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
        val partitionOffsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
        jedis.close()
        if(partitionOffsetMap!=null&&partitionOffsetMap.size()>0){
            javaMapToScalaMap(topic,partitionOffsetMap)
        }else{
            //println("没有取到偏移量")
            null
        }
    }

    def saveOffset(topic:String,groupId:String,offsetRanges: Array[OffsetRange]): Unit ={
        val jedis: Jedis = RedisUtil.getJedisClient
        val offsetKey = topic + groupId
        val map: util.HashMap[String, String] = new util.HashMap[String, String]
        for(offsetRange <- offsetRanges){
            //println(s"保存 分区：${offsetRange.partition} 偏移量：${offsetRange.untilOffset}")
            map.put(offsetRange.partition.toString,offsetRange.untilOffset.toString)
        }
        jedis.hmset(offsetKey,map)
    }

    def javaMapToScalaMap(topic:String ,partitionOffsetMap: util.Map[String, String]): Map[TopicPartition,Long]={
        val targetMap: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
        partitionOffsetMap.forEach{
            case (patition,offset)=>{
                //println(s"分区：${patition}, 偏移量：${offset}")
                targetMap.put(new TopicPartition(topic,patition.toInt),offset.toLong)
            }
        }
        targetMap.toMap
    }

}
