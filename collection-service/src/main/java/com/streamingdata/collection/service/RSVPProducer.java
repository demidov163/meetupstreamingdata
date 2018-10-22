package com.streamingdata.collection.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class RSVPProducer {
    private static KafkaProducer<byte[], byte[]> kafkaProducer;
    private static final String messageTopic = "meetup-raw-rsvps";

    public RSVPProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "meetup-collection-service-kafka");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProducer = new KafkaProducer<>(producerProperties);

    }

    public void sendMessage(String messageKey, byte[] messagePayload) {
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(messageTopic, messageKey.getBytes(),
                messagePayload);
        System.out.println("RSVPProducer sendMessage send ");
        kafkaProducer.send(producerRecord, new TopicCallbackHandler(messageKey));

    }

    public void close() {
        kafkaProducer.close();
    }
}
