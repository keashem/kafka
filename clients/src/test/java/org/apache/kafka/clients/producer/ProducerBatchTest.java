package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

/**
 * @author keashem@163.com
 * @date 2020/12/29 5:15 下午
 */
public class ProducerBatchTest {
    private static KafkaConsumer<String, String> consumer;
    private static KafkaProducer<String, String> producer;
    private static String batchTopic = "0104topic2";

    @Before
    public void init(){
        String brokerList = "localhost:9092";
        String consumerTopic = "0104topic1";
        String groupId = "0104group1";
        Properties propConsumer = new Properties();
        propConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        propConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        propConsumer.put("bootstrap.servers", brokerList);
        propConsumer.put("group.id", groupId);
        propConsumer.put("auto.offset.reset", "earliest");
        consumer = new KafkaConsumer<>(propConsumer);
        consumer.subscribe(Collections.singletonList(consumerTopic));

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    @Test
    public void testNoCallBackSend(){
        try {
            while (!Thread.interrupted()) {
                RecordBatch batch = consumer.pollForBatch(Duration.ofMillis(1000));
                if(Objects.nonNull(batch)){
                    System.out.println(batch);
                    producer.send(batch,batchTopic);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            producer.close();
        }
    }

    @Test
    public void testCallBackSend(){
        try {
            while (!Thread.interrupted()) {
                RecordBatch batch = consumer.pollForBatch(Duration.ofMillis(1000));
                if(Objects.nonNull(batch)){
                    System.out.println(batch);
                    producer.send(batch, batchTopic, (metadata, exception) -> {
                        System.out.println("callback method invokes while producerBatch sends over");
                    });
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
