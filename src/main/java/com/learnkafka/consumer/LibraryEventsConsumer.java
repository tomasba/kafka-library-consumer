package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventsConsumer {

    Logger log = LoggerFactory.getLogger(LibraryEventsConsumer.class);

    @KafkaListener(topics = {"${spring.kafka.topic:library-events}"})
    public void onMessage(ConsumerRecord<Integer, String> record) {
        log.info("Received Library Events record: {}", record);
    }

}
