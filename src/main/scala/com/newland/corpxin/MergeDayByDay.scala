package com.newland.corpxin

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import org.json.JSONObject
import org.json.JSONArray
import org.json.JSONException


/**
 * @Author: sober  2020-07-27 16:09
 */
object MergeDayByDay {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkSession.builder().appName("MergeDayByDay").master("local[3]").getOrCreate().sparkContext

    val rdd1: RDD[String] = sc.textFile("D:\\inputdata\\20200724\\flume.1595831409544.txt")

    val rdd2: RDD[(AnyRef, AnyRef, AnyRef, String)] = rdd1.map(line => {
      val jsonObj = new JSONObject(line)
      val xwhat = jsonObj.get("xwhat")
      val xwho = jsonObj.get("xwho")
      val xtime = jsonObj.get("xtime")
      (xwhat, xwho, xtime, line)
    })






  }

}
