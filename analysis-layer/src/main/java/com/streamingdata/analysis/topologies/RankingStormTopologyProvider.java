package com.streamingdata.analysis.topologies;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamingdata.analysis.KafkaSpoutFactory;
import com.streamingdata.analysis.bolts.stormtopology.TotalRankingBolt;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.FlatMapFunction;
import org.apache.storm.streams.operations.mappers.PairValueMapper;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.io.IOException;
import java.util.List;

public class RankingStormTopologyProvider {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final FlatMapFunction<Pair<Object, String>, String> EXTRACT_GROUPS_FUNCTION = new FlatMapFunction<Pair<Object, String>, String>() {
         @Override
         public Iterable<String> apply(Pair<Object, String> objectObjectPair) {
             List<String> topicNames = Lists.newArrayList();
             try {
                 JsonNode jsonMessage = objectMapper.readTree(objectObjectPair._2);
                 JsonNode groupTopics = jsonMessage.get("group").get("group_topics");
                 for (JsonNode groupTopic : groupTopics) {
                     String topicName = groupTopic.get("topic_name").asText();
                     if (StringUtils.isNotEmpty(topicName)) {
                         topicNames.add(topicName);
                     }
                 }
             } catch (IOException e) {
                 e.printStackTrace();
             }
              return topicNames;
         }
     };
    private KafkaSpoutFactory kafkaSpoutFactory;

    public RankingStormTopologyProvider() {
        kafkaSpoutFactory = new KafkaSpoutFactory();
    }

    public StormTopology provideStormTopology() {
        StreamBuilder builder = new StreamBuilder();
        builder.newStream(kafkaSpoutFactory.getKafkaSpout(),
                new PairValueMapper<Object, String>(3, 4), 1).
                window(getKafkaWindow()).flatMap(EXTRACT_GROUPS_FUNCTION).
                mapToPair(s -> Pair.of(s, 1)).
                countByKey().to(new TotalRankingBolt());


        return builder.build();
    }

    private SlidingWindows<BaseWindowedBolt.Duration, BaseWindowedBolt.Duration> getKafkaWindow() {
        return SlidingWindows.of(BaseWindowedBolt.Duration.minutes(3), BaseWindowedBolt.Duration.seconds(30));
    }

}
