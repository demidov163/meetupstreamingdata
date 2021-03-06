package com.streamingdata.analysis;

public class TopMeetupTopicsTopology {
    /*private static final String TOPOLOGY_NAME = "TopMeetupTopics";
    private static final String TOPIC_NAME = "meetup-raw-rsvps";
    private static final String STREAM_NAME = "meetup-rsvp-stream";

    private void runLocally() throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, getConfig(), buildTopology());
        stopWaitingForInput();
    }

    private void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP THE TOPOLOGY");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void runRemotely() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        StormSubmitter.submitTopology(TOPOLOGY_NAME, getConfig(), buildTopology());
    }

    private Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    private StormTopology buildTopology() {
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig()), 1);
        topologyBuilder.setBolt("rsvpSummarizer", new MeetupRSVPSummaryBolt(), 1).globalGrouping("kafka_spout", STREAM_NAME);
        topologyBuilder.setBolt("summarySerializer", new KafkaBolt(), 1).shuffleGrouping("rsvpSummarizer");
        return topologyBuilder.createTopology();
    }

    private KafkaSpoutConfig<String, String> getKafkaSpoutConfig() {
        return new KafkaSpoutConfig.Builder<>(getKafkaConsumerProps(),
                getKafkaSpoutStreams(),
                getTuplesBuilder(),
                getRetryService())
                .build();
    }

    private KafkaSpoutStreams getKafkaSpoutStreams() {
        final Fields outputFields = new Fields("topic", "partition", "offset", "key", "value");
        return new KafkaSpoutStreamsNamedTopics.Builder(outputFields, STREAM_NAME, new String[]{TOPIC_NAME})
                .build();
    }

    private Map<String, Object> getKafkaConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(KafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT, "true");
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, TOPIC_NAME + "-group");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private KafkaSpoutTuplesBuilder<String, String> getTuplesBuilder() {
        return new KafkaSpoutTuplesBuilderNamedTopics.Builder<>(new TopicTupleBuilder(TOPIC_NAME)).build();
    }

    private KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

    public static void main(String[] args) throws Exception {

        boolean runLocally = true;
        if (args.length >= 1 && args[0].equalsIgnoreCase("remote")) {
            runLocally = false;
        }

        TopMeetupTopicsTopology topMeetupTopicsTopology = new TopMeetupTopicsTopology();
        if (runLocally) {
            System.out.println("Running in local mode");
            topMeetupTopicsTopology.runLocally();
        } else {
            System.out.println("Running in remote (cluster) mode");
            topMeetupTopicsTopology.runRemotely();
        }
    }*/
}
