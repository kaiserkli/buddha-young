package cn.aura.buddha.young.bigdata.streaming;

import cn.aura.buddha.young.bigdata.util.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 任务5
 * 主要完成发送数据到Kafka
 */
public class KafkaEventProducer {

    public static void main(String[] args) throws InterruptedException {

        String host = "172.16.186.128";
        String hdfs = "warehouse/buddha_young.db/user_pay";
        String path = "./buddha-young-bigdata/data/user_pay.txt";

        if (args.length == 2) {
            host = args[0];
            hdfs = args[1];
            path = "hdfs://" + host + ":9000/" + hdfs;
        }

        SparkConf conf = new SparkConf().setAppName("KafkaEventProducer");
        JavaSparkContext spark = new JavaSparkContext(conf);

        //读取用户支付信息
        JavaRDD<String> input = spark.textFile(path);

        JavaRDD<String[]> userPays = input.map(new Function<String, String[]>() {

            @Override
            public String[] call(String line) throws Exception {
                return line.split(",");
            }
        });


        String topic = ConfigUtil.KAFKA_TOPIC;
        String brokers = host + ":9092";

        //Kafka Producer 配置信息
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        //props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        //props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

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
