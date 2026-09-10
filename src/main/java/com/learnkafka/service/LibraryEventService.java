package com.learnkafka.service;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.repo.LibraryEventRepo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class LibraryEventService {

    private final Logger log = LoggerFactory.getLogger(LibraryEventService.class);

    private final LibraryEventRepo libraryEventRepo;
    private final ObjectMapper objectMapper;

    public LibraryEventService(LibraryEventRepo libraryEventRepo, ObjectMapper objectMapper) {
        this.libraryEventRepo = libraryEventRepo;
        this.objectMapper = objectMapper;
    }

    public void process(ConsumerRecord<Integer, String> record) {
        var libraryEvent = objectMapper.readValue(record.value(), LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);

        switch(libraryEvent.getLibraryEventType()) {
            case NEW ->  save(libraryEvent);
            case UPDATE -> libraryEventRepo.save(libraryEvent);
            default -> log.warn("Invalid library event type provided {}. Can not process the event into database",
                    libraryEvent.getLibraryEventType());
        }
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        // kind-a-hack actually book should exist already before creating an event
        libraryEvent.getBook().setBookId(null);
        libraryEventRepo.save(libraryEvent);
        log.info("Library event saved for book {}", libraryEvent.getBook());
    }

    private void update(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepo.save(libraryEvent);
        log.info("Library event updated for book {}", libraryEvent.getBook());
    }

}
