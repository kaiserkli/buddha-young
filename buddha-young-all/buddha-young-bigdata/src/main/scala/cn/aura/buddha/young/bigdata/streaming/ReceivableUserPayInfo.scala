package cn.aura.buddha.young.bigdata.streaming

import cn.aura.buddha.young.bigdata.util.{ConfigUtil, RedisUtil}
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

    var host = "172.16.186.128"
    var hdfs = "warehouse/buddha_young.db/shop_info"
    var path = "./buddha-young-bigdata/data/shop_info.txt"

    if (args.length == 1) {
      host = args(0)
      hdfs = args(1)
      path = s"hdfs://${host}:9000/${hdfs}"
    }

    val checkPointDir = s"hdfs://${host}:9000/buddha_young/checkpoint"

    val stream = StreamingContext.getOrCreate(checkPointDir, () => {createContext(checkPointDir, host, path)})

    stream.start()
    stream.awaitTermination()
  }

  def createContext(checkPointDir: String, host: String, path: String): StreamingContext = {

    val conf = new SparkConf().setAppName("ReceivableUserPayInfo")
    val stream = new StreamingContext(conf, Durations.seconds(1))
    val shopInfos = stream.sparkContext.textFile(path).map(_.split(",")).map(line => (line(0), line(1)))

    shopInfos.cache()

    //Kafka Consumer数据读取参数
    val topic = ConfigUtil.KAFKA_TOPIC
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> s"${host}:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> ConfigUtil.KAFKA_GROUP_ID,
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
        val jedis = RedisUtil.getJedis(host)

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
        val jedis = RedisUtil.getJedis(host)

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
