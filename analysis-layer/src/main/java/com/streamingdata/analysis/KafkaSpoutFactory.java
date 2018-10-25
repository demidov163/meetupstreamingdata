package com.streamingdata.analysis;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

public class KafkaSpoutFactory {
    private static final String TOPIC_NAME = "meetup-raw-rsvps";
    private static final String STREAM_NAME = "meetup-rsvp-stream";

    public KafkaSpout getKafkaSpout() {

        KafkaSpoutConfig.Builder<String, String> configBuilder = KafkaSpoutConfig.builder("127.0.0.1:" + 9092, "meetup-raw-rsvp");
        configBuilder.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);

        return new KafkaSpout<>(configBuilder.build());
    }
}
