package com.atguigu.analysis.server.app

import java.util.Calendar

import com.alibaba.fastjson.JSON
import com.atguigu.analysis.server.bean.StartLog
import com.atguigu.analysis.server.util.RedisUtil
import redis.clients.jedis.Jedis

object MyTest {

    def main(args: Array[String]): Unit = {
       /* val str = "{\"common\":{\"ar\":\"440000\",\"ba\":\"iPhone\",\"ch\":\"Appstore\",\"md\":\"iPhone Xs Max\",\"mid\":\"mid_70\",\"os\":\"iOS 13.2.9\",\"uid\":\"371\",\"vc\":\"v2.1.134\"},\"start\":{\"entry\":\"icon\",\"loading_time\":4511,\"open_ad_id\":2,\"open_ad_ms\":6926,\"open_ad_skip_ms\":6731},\"ts\":1598351203000}";
        val startLog: StartLog = JSON.parseObject(str, StartLog.getClass)
        println(startLog)*/
       /*val jedis: Jedis = RedisUtil.getJedisClient
        val str: String = jedis.getSet("ccc", "1")
        if(str==null){
            println("1")
        }else{
            println("2")
        }
        println(str)
        jedis.close()*/
        val cal: Calendar = Calendar.getInstance()
        cal.setTimeInMillis(1598351203000L)
        val year: Int = cal.get(Calendar.YEAR)
        val month: Int = cal.get(Calendar.MONTH) + 1
        val day: Int = cal.get(Calendar.DAY_OF_MONTH)
        val hour: Int = cal.get(Calendar.HOUR_OF_DAY)
        val dayStr: String = s"${year}-${month}-${day}"
        val hourStr: String = String.valueOf(hour)
        println(dayStr)
        println(hourStr)
    }

}
