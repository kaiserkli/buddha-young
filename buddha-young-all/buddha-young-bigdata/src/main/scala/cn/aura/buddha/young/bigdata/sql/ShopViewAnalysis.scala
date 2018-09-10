package cn.aura.buddha.young.bigdata.sql

import java.util

import cn.aura.buddha.young.bigdata.entity.ShopView
import cn.aura.buddha.young.bigdata.util.ConfigUtil
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._

/**
  * 任务4
  * 第三、四子任务
  */
object ShopViewAnalysis {

  def main(args: Array[String]): Unit = {
    viewByTimeAnalysis(1198, 1)
    //viewByTop50Analysis()
  }

  /**
    * 给定一个商店(可动态指定)，输出该商店每天、每周和每月的被浏览数量
    *
    * @param shopId 商家ID
    * @param dateType 数据类型 1为按日 2为按周 3为按月
    * @return List[ShopView]
    */
  def viewByTimeAnalysis(shopId: Int, dateType: Int): util.List[ShopView] = {
    val spark = SparkSession.builder().master("local[*]").appName("ViewByTimeAnalysis").getOrCreate()

    val input = spark.sparkContext.textFile(ConfigUtil.HADOOP_HDFS_URL + "/warehouse/buddha_young.db/user_view").map(_.split(",", -1)).map(line => transformUserView(line));

    val structType = StructType(Array(StructField("shop_id", DataTypes.IntegerType, true), StructField("time_stamp", DataTypes.StringType, true)))

    val userViews = spark.createDataFrame(input, structType).filter(s"shop_id = ${shopId}")

    var result = new Array[Row](0)

    if (dateType == 1) {
      result = userViews.select("shop_id", "time_stamp").groupBy("time_stamp").agg(count("shop_id").alias("view_count")).sort(asc("time_stamp")).collect()
    } else if (dateType == 2) {
      result = userViews.withColumn("time_stamp", concat(substring(col("time_stamp"), 0, 5), weekofyear(col("time_stamp")))).select("shop_id", "time_stamp").groupBy("time_stamp").agg(count("shop_id").alias("view_count")).sort(asc("time_stamp")).collect()
    } else if (dateType == 3) {
      result = userViews.withColumn("time_stamp", substring(col("time_stamp"), 0, 7)).select("shop_id", "time_stamp").groupBy("time_stamp").agg(count("shop_id").alias("view_count")).sort(asc("time_stamp")).collect()
    }

    result.foreach(println)

    val list = new util.ArrayList[ShopView]()

    for (row <- result) {
      val shopView = new ShopView()
      shopView.setTimestamp(row.getString(0))
      shopView.setViewCount(row.getLong(1))

      list.add(shopView)
    }

    spark.stop()

    return list
  }

  /**
    * 找到被浏览次数最多的50个商家，并输出他们的城市以及人均消费
    * @return List[ShopView]
    */
  def viewByTop50Analysis(): util.List[ShopView] = {
    val spark = SparkSession.builder().master("local[*]").appName("ViewByTimeAnalysis").getOrCreate()

    val inputShopInfo = spark.sparkContext.textFile("./buddha-young-bigdata/data/shop_info.txt").map(_.split(",", -1)).map(line => transformShopInfo(line));
    val inputUserView = spark.sparkContext.textFile("./buddha-young-bigdata/data/user_view.txt").map(_.split(",", -1)).map(line => transformUserView(line));

    val structTypeShopInfo = StructType(Array(StructField("shop_id", DataTypes.IntegerType, true), StructField("city_name", DataTypes.StringType, true), StructField("per_pay", DataTypes.DoubleType, true)))
    val structTypeUserView = StructType(Array(StructField("shop_id", DataTypes.IntegerType, true), StructField("time_stamp", DataTypes.StringType, true)))

    val shopInfos = spark.createDataFrame(inputShopInfo, structTypeShopInfo)
    val userViews = spark.createDataFrame(inputUserView, structTypeUserView)

    val shopViews = shopInfos.join(userViews, "shop_id").select("shop_id").groupBy("shop_id").agg(count("shop_id").alias("view_count")).sort(desc("view_count")).limit(50)

    val shopViewTop50 = shopViews.join(shopInfos, "shop_id").select("shop_id", "city_name", "per_pay", "view_count").collect()

    val list = new util.ArrayList[ShopView]()

    for (row <- shopViewTop50) {
      val shopView = new ShopView()
      shopView.setShopId(row.getInt(0))
      shopView.setCityName(row.getString(1))
      shopView.setPerPay(row.getDouble(2))
      shopView.setViewCount(row.getLong(3))

      list.add(shopView)
    }

    spark.stop()

    return list
  }

  /**
    * 商家信息转换
    * @param line 数据
    * @return Row
    */
  def transformShopInfo(line: Array[String]): Row = {
    val shopId = line(0).trim.toInt
    val cityName = line(1).trim
    val perPay = line(3).trim.toDouble

    Row(shopId, cityName, perPay)
  }

  /**
    * 用户浏览信息转换
    * @param line 数据
    * @return Row
    */
  def transformUserView(line: Array[String]): Row = {
    val shopId = line(1).trim.toInt
    val timestamp = line(2).trim.split(" ")(0)

    Row(shopId, timestamp)
  }
}
