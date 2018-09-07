package cn.aura.buddha.young.bigdata.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Function2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 任务5
 * 主要完成发送书记到Kafka
 */
public class KafkaEventProducer {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("KafkaEventProducer").setMaster("local[2]");
        JavaSparkContext spark = new JavaSparkContext(conf);

        //读取用户支付信息
        //JavaRDD<String> input = spark.textFile("hdfs://192.168.21.74:9000/warehouse/buddhalike.db/user_pay");
        JavaRDD<String> input = spark.textFile("./data/user_pay.txt");

        JavaRDD<String[]> userPays = input.map(new Function<String, String[]>() {

            @Override
            public String[] call(String line) throws Exception {
                return line.split(",");
            }
        });


        String topic = "user_pay";
        String brokers = "172.16.186.128:9092";

        //Kafka Producer 配置信息
        Map<String, Object> props = new HashMap<>();
        //props.put("zookeeper.connect", "172.16.186.128:2181");
        props.put("metadata.broker.list", brokers);
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        //props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("group.id", "user_pay");
        //props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //发送Partition中的数据到Kafka
        userPays.foreachPartition(new VoidFunction<Iterator<String[]>>() {

            @Override
            public void call(Iterator<String[]> it) throws Exception {

                KafkaProducer producer = new KafkaProducer(props);
                while (it.hasNext()) {
                    String[] data = it.next();

                    ProducerRecord producerRecord = new ProducerRecord<String, String>(topic, data[0],data[1] + "," + data[2]);
                    producer.send(producerRecord);

                    System.out.println(producerRecord.toString());

                    Thread.sleep(10);
                }
            }
        });

        spark.stop();
    }
}
