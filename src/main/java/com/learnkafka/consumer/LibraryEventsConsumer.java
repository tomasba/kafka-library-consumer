package com.learnkafka.consumer;

import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventsConsumer {

    Logger log = LoggerFactory.getLogger(LibraryEventsConsumer.class);

    private final LibraryEventsService libraryEventsService;

    public LibraryEventsConsumer(LibraryEventsService libraryEventsService) {
        this.libraryEventsService = libraryEventsService;
    }

    @KafkaListener(topics = {"${spring.kafka.topic:library-events}"})
    public void onMessage(ConsumerRecord<Integer, String> record) {
        log.info("Received Library Events record: {}", record);
        libraryEventsService.process(record);
    }

}
