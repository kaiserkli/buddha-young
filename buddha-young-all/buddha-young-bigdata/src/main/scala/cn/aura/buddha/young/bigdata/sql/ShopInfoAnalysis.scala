package cn.aura.buddha.young.bigdata.sql

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._

/**
  * 任务4
  * 第一、二子任务
  */
object ShopInfoAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("SparkTask04Scala").getOrCreate();

    //val inputShopInfo = spark.sparkContext.textFile("hdfs://192.168.21.74:9000/warehouse/buddhalike.db/shop_info").map(_.split(",", -1)).map(line => exchangeShopInfo(line));

    //val inputUserPay = spark.sparkContext.textFile("hdfs://192.168.21.74:9000/warehouse/buddhalike.db/user_pay").map(_.split(",", -1)).map(line => exchangeUserPay(line));


    val inputShopInfo = spark.sparkContext.textFile("./data/shop_info.txt").map(_.split(",", -1)).map(line => exchangeShopInfo(line));

    val inputUserPay = spark.sparkContext.textFile("./data/user_pay.txt").map(_.split(",", -1)).map(line => exchangeUserPay(line));


    val shopInfoStructType = StructType(Array(StructField("shop_id", DataTypes.IntegerType, true), StructField("city_name", DataTypes.StringType, true),
      StructField("location_id", DataTypes.IntegerType, true), StructField("per_pay", DataTypes.DoubleType, true), StructField("score", DataTypes.DoubleType, true),
      StructField("comment_cnt", DataTypes.IntegerType, true), StructField("shop_level", DataTypes.IntegerType, true),
      StructField("cate_1_name", DataTypes.StringType, false), StructField("cate_2_name", DataTypes.StringType, false), StructField("cate_3_name", DataTypes.StringType, false)))

    val userPayStructType = StructType(Array(StructField("user_id", DataTypes.IntegerType, true), StructField("shop_id", DataTypes.IntegerType, true), StructField("time_stamp", DataTypes.StringType, true)))

    val shopInfo = spark.createDataFrame(inputShopInfo, shopInfoStructType)
    shopInfo.cache()

    val userPay = spark.createDataFrame(inputUserPay, userPayStructType)

    task01(shopInfo, userPay)
    task02(shopInfo)

    spark.stop()
  }

  /**
    * 平均日交易额最大的前10个商家，并输出他们各自的交易额
    * @param shopInfo 商家信息
    * @param userPay 用户支付信息
    */
  def task01(shopInfo: Dataset[Row], userPay: Dataset[Row]): Unit = {
    val shopPayCount = userPay.select("shop_id", "time_stamp").groupBy("time_stamp", "shop_id").agg(count("shop_id").alias("pay_count"))
    val shopPayAndDateCount = shopPayCount.select("shop_id", "pay_count").groupBy("shop_id").agg(sum("pay_count").alias("pay_times"), count("shop_id").alias("date_times"))
    val shopPayJoin = shopPayAndDateCount.join(shopInfo, "shop_id").select("shop_id", "date_times", "pay_times", "per_pay")
    shopPayJoin.cache()

    val saleTop10 = shopPayJoin.withColumn("avg_total", col("pay_times") * col("per_pay") / col("date_times")).select("shop_id", "avg_total").sort(desc("avg_total")).limit(10)

    saleTop10.show()
  }

  /**
    * 输出北京、上海、广州和深圳四个城市最受欢迎的5家奶茶商店和中式快餐编号(这两个分别输出出来)
    * 最受欢迎是指以下得分最高:0.7✖(平均评分/5)+0.3✖(平均消费金额/最高消费金额)，注:最高消费金额和平均消费金额是从所有消费记录统计出来的
    * @param shopInfo 商家信息
    */
  def task02(shopInfo: Dataset[Row]): Unit = {
    val milkTeaData = shopInfo.filter("(city_name = '北京' or city_name = '上海' or city_name = '广州' or city_name = '深圳') and cate_3_name = '奶茶'")
    val fastFoodData = shopInfo.filter("(city_name = '北京' or city_name = '上海' or city_name = '广州' or city_name = '深圳') and cate_3_name = '中式快餐'")

    val maxPayByCate = shopInfo.groupBy("cate_3_name").agg(max("per_pay").alias("max_pay"))

    milkTeaData.join(maxPayByCate, "cate_3_name").withColumn("popularity", col("score") / 5 * 0.7 + col("per_pay") / col("max_pay") * 0.3).sort(desc("popularity")).limit(5).select("shop_id", "city_name", "cate_3_name", "popularity").show()

    fastFoodData.join(maxPayByCate, "cate_3_name").withColumn("popularity", col("score") / 5 * 0.7 + col("per_pay") / col("max_pay") * 0.3).sort(desc("popularity")).limit(5).select("shop_id", "city_name","cate_3_name", "popularity").show()

  }

  def task03(): Unit = {

  }

  def exchangeShopInfo(line: Array[String]): Row = {
    val shopId = line(0).trim.toInt
    val cityName = line(1)
    val locationId = line(2).trim.toInt
    var perPay = 0.0

    if (line(3) != null && !line(3).equals("")) {
      perPay = line(3).trim.toDouble
    }

    var score = 0.0

    if (line(4) != null && !line(4).equals("")) {
      score = line(4).trim.toDouble
    }

    var commentCnt = 0

    if (line(5) != null && !line(5).equals("")) {
      commentCnt = line(5).trim.toInt
    }

    val shopLevel = line(6).trim.toInt

    val cate1name = line(7)
    val cate2name = line(8)
    val cate3name = line(9)

    Row(shopId, cityName, locationId, perPay, score, commentCnt, shopLevel, cate1name, cate2name, cate3name)
  }

  def exchangeUserPay(line: Array[String]): Row = {
    val userId = line(0).trim.toInt
    val shopId = line(1).trim.toInt
    val timestamp = line(2).split(" ")(0)

    Row(userId, shopId, timestamp)
  }
}
