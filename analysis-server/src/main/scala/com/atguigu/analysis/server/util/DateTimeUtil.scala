package com.atguigu.analysis.server.util

import java.text.SimpleDateFormat
import java.util.Calendar

object DateTimeUtil {

    var format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var formatTime: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    var newFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    var cal : Calendar = Calendar.getInstance()

    def  getDayStr(cal:Calendar): String={
        val year: Int = cal.get(Calendar.YEAR)
        val month: Int = cal.get(Calendar.MONTH) + 1
        val day: Int = cal.get(Calendar.DAY_OF_MONTH)
        s"${year}-${month}-${day}"
    }

    def getHourStr(cal:Calendar): String={
        val hour: Int = cal.get(Calendar.HOUR_OF_DAY)
        String.valueOf(hour)
    }

    def getMiStr(cal:Calendar): String={
        val minute: Int = cal.get(Calendar.MINUTE)
        String.valueOf(minute)
    }

    def getAge(year:Int): Int={
        cal.setTimeInMillis(System.currentTimeMillis())
        cal.get(Calendar.YEAR) - year
    }

    def getAgeGroup(age:Int): String={
        if(age >0 && age <= 6) {
            "婴幼儿"
        }else if (age <= 12){
            "少儿"
        }else if (age <= 17){
            "青少年"
        }else if (age <= 45){
            "青年"
        }else if (age <= 69){
            "中年"
        }else{
            "老年"
        }
    }

}
