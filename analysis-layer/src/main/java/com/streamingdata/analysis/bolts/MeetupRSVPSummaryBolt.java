package com.streamingdata.analysis.bolts;

import com.clearspring.analytics.stream.StreamSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MeetupRSVPSummaryBolt extends BaseRichBolt {
    private static final long serialVersionUID = 6749134782408285639L;
    private static StreamSummary<String> streamSummary = new StreamSummary<>(100000);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private OutputCollector collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
         this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println(String.format("MeetupRSVPSummaryBolt execute start %s", tuple.toString()));
        String jsonString = tuple.getString(4);
        try {
            JsonNode root = objectMapper.readTree(jsonString);
            JsonNode groupTopics = root.get("group").get("group_topics");
            for (JsonNode groupTopic : groupTopics) {
                String topic_name = groupTopic.get("topic_name").asText();
                streamSummary.offer(topic_name);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            List<Integer> emit = collector.emit(new Values(UUID.randomUUID().toString(),
                    objectMapper.writeValueAsString(streamSummary.topK(5))));
            System.out.println(String.format("MeetupRSVPSummaryBolt execute end %s", emit.toString()));
            collector.ack(tuple);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("objid", "summary"));
    }


}
