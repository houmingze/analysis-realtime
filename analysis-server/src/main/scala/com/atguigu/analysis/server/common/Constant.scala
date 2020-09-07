package com.atguigu.analysis.server.common

import scala.collection.mutable.ArrayBuffer

object Constant {

    val KAFKA_TOPIC_GMALL_START = "GMALL_START";
    val KAFKA_TOPIC_GMALL_EVENT = "GMALL_EVENT";

    val GROUP_CONSUMER_START_COUNT = "GMALL_DAU_START"

    val KEY_PRE_START_COUNT = "START_COUNT_"
    val VALUE_START_COUNT = "1"

    val TABLE_NAMES = ArrayBuffer[String]("order_info","order_detail")

}
