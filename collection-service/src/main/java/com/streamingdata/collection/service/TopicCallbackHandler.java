package com.streamingdata.collection.service;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TopicCallbackHandler implements Callback {
    final String eventKey;

    public TopicCallbackHandler(String messageKey) {
        eventKey = messageKey;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println("TopicCallbackHandler onCompletion recordMetadata " + recordMetadata + " Exception " + e);
        if (recordMetadata == null) {
            //means that error is occurred. mark record as erroneous
            HybridMessageLogger.moveToFailed(eventKey);
        } else {
            HybridMessageLogger.removeEvent(eventKey);
        }
    }
}
