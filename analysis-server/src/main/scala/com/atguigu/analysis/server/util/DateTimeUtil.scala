package com.atguigu.analysis.server.util

import java.util.Calendar

object DateTimeUtil {
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
}
