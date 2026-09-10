package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Objects;

//@Component
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

    Logger log = LoggerFactory.getLogger(LibraryEventsConsumerManualOffset.class);

    @Override
    @KafkaListener(topics = {"${spring.kafka.topic:library-events}"})
    public void onMessage(ConsumerRecord<Integer, String> record, Acknowledgment acknowledgment) {
        log.info("Received Library Events record with manual ACK: {}", record);
        Objects.requireNonNull(acknowledgment, "Acknowledgment must not be null");
        acknowledgment.acknowledge();
    }

}
