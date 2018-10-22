package com.streamingdata.analysis.bolts;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Properties;

public class KafkaBolt extends BaseRichBolt {
    private static final long serialVersionUID = 4700085881296807569L;
    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private OutputCollector collector;
    private static final String topNTopicName = "meetup-topn-rsvps";

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "meetup-analyis-service-topn-bolt");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducer = new KafkaProducer<>(producerProperties);
    }

    public void execute(Tuple tuple) {
        System.out.println(String.format("KafkaBolt execute start %s", tuple.toString()));
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topNTopicName,
                tuple.getString(0).getBytes(), tuple.getString(1).getBytes());
        kafkaProducer.send(producerRecord);
        System.out.println(String.format("KafkaBolt execute end %s"));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        super.cleanup();
        kafkaProducer.close();
    }
}
