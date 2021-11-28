package cn.edu.sysu.util;

import cn.edu.sysu.bean.TableProcess;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.reflect.Nullable;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author : song bei chang
 * @create 2021/11/27 10:09
 */
public class MyKafkaUtil {

    /**
     * kafka source
     *
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId,
                                                            String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "ecs2:9092,ecs3:9092,ecs4:9092");
        props.setProperty("group.id", groupId);
        // 如果启动的时候, 这个消费者对这个topic的消费没有上次的消费记录, 就从这个配置的位置开始消费
        // 如果有消费记录, 则从上次的位置开始消费 earliest  latest  none
        props.setProperty("auto.offset.reset", "earliest");
        // 设置隔离级别
        props.setProperty("isolation.level", "read_committed");
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
    }


    /**
     * 普通版(适用于log)
     * kafka sink
     *
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ecs2:9092,ecs3:9092,ecs4:9092");
        // kafka 和 flink 的事务时间不一致 60 15min
        properties.setProperty("transaction.timeout.ms", 1000 * 60 * 15 + "");

        return new FlinkKafkaProducer<String>(
                topic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(topic, null, element.getBytes());
                    }
                },
                properties,
                // 开启Flink Kafka的二阶段提交
                //FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                // Using EXACTLY_ONCE semantic, but checkpointing is not enabled. Switching to NONE semantic.
                FlinkKafkaProducer.Semantic.NONE
        );

    }



    /**
     *  升级版(适用于db)
     *  kafka sink
     *  根据内容动态的写入不同的kafka Topic (本质是提取SinkTable)
     */
    public static FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>> getKafkaSink() {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "ecs2:9092,ecs3:9092,ecs4:9092");
        //如果15分钟没有更新状态，则超时 默认1分钟
        props.setProperty("transaction.timeout.ms", 1000 * 60 * 15 + "");

        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
                // 为什么default_topic 没有数据呢？
                "default_topic",
                new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> element, @Nullable Long timestamp) {
                        final String topic = element.f1.getSinkTable();
                        // System.out.println("topic:-->"+topic);
                        // maxwell
                        final JSONObject data = element.f0.getJSONObject("data");
                        // 动态 topic
                        return new ProducerRecord<>(topic, data.toJSONString().getBytes());
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }




}



