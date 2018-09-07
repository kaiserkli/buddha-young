package cn.aura.buddha.young.bigdata.streaming

import cn.aura.buddha.young.bigdata.util.RedisUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Durations, StreamingContext}
import net.sourceforge.pinyin4j.PinyinHelper
import net.sourceforge.pinyin4j.format.{HanyuPinyinCaseType, HanyuPinyinOutputFormat, HanyuPinyinToneType}

/**
  * 任务5
  * 通过Spark Streaming读取Kafka中数据进行计算
  */
object ReceivableUserPayInfo {

  val pinyinFormat = new HanyuPinyinOutputFormat();

  def main(args: Array[String]): Unit = {

    val checkPointDir = "hdfs://172.16.186.128:9000/tmp/checkpoint"

    //val stream = StreamingContext.getOrCreate(checkPointDir, () => {createContext(checkPointDir)})
    val stream = createContext(checkPointDir)

    stream.start()
    stream.awaitTermination()
  }

  def createContext(checkPointDir: String): StreamingContext = {
    val conf = new SparkConf().setAppName("ReceivableUserPayInfo").setMaster("local[2]")
    val stream = new StreamingContext(conf, Durations.seconds(1))
    //val shopInfos = stream.sparkContext.textFile("hdfs://192.168.21.74:9000/warehouse/buddhalike.db/shop_info").map(_.split(",")).map(line => (line(0), line(1)))
    val shopInfos = stream.sparkContext.textFile("./data/shop_info.txt").map(_.split(",")).map(line => (line(0), line(1)))


    shopInfos.cache()

    //Kafka Consumer数据读取参数
    val topic = "user_pay"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.16.186.128:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "user_pay",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //读取数据
    val lines = KafkaUtils.createDirectStream[String, String](stream, PreferConsistent, Subscribe[String, String](Set(topic), kafkaParams)).map(_.value())

    lines.foreachRDD(rdd => {
      val userPays = rdd.map(_.split(","))
      val payCount = userPays.map(line => (line(0), 1)).reduceByKey(_+_)

      //计算商家支付次数，并保存到Redis
      payCount.foreach(shopPay => {
        val jedis = RedisUtil.getJedis

        val shopId = shopPay._1
        val count = shopPay._2

        if (jedis.exists(s"Trade<${shopId}>")) {
          val total = count + jedis.get(s"Trade<${shopId}>").toInt
          jedis.set(s"Trade<${shopId}>", String.valueOf(total))
        } else {
          jedis.set(s"Trade<${shopId}>", String.valueOf(count))
        }

        jedis.close()
      })

      //计算同一地区商家支付总数，并保存到Redis
      val cityCount = shopInfos.join(payCount).map(line => (line._2._1, line._2._2)).reduceByKey(_+_)

      cityCount.foreach(result => {
        val jedis = RedisUtil.getJedis

        pinyinFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE)
        pinyinFormat.setCaseType(HanyuPinyinCaseType.UPPERCASE)

        val city = result._1
        val count = result._2

        val pinyin = PinyinHelper.toHanYuPinyinString(city, pinyinFormat, "", false)

        if (jedis.exists(s"Trade<${pinyin}>")) {
          val total = count + jedis.get(s"Trade<${pinyin}>").toInt
          jedis.set(s"Trade<${pinyin}>", String.valueOf(total))
        } else {
          jedis.set(s"Trade<${pinyin}>", String.valueOf(count))
        }
        jedis.close()
      })
    })

    stream
  }
}
