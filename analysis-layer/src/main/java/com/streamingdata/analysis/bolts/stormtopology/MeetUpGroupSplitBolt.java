package com.streamingdata.analysis.bolts.stormtopology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.Map;

public final class MeetUpGroupSplitBolt extends BaseRichBolt{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private OutputCollector collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
         this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String jsonString = tuple.getString(4);
        try {
            JsonNode jsonMessage = objectMapper.readTree(jsonString);
            JsonNode groupTopics = jsonMessage.get("group").get("group_topics");
            for (JsonNode groupTopic : groupTopics) {
                String topicName = groupTopic.get("topic_name").asText();
                collector.emit(new Values(topicName));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
         outputFieldsDeclarer.declare(new Fields("groupname"));
    }
}
