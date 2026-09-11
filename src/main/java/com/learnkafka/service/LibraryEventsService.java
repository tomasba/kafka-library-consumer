package com.learnkafka.service;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.repo.BookRepo;
import com.learnkafka.repo.LibraryEventRepo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class LibraryEventsService {

    private final Logger log = LoggerFactory.getLogger(LibraryEventsService.class);

    private final LibraryEventRepo libraryEventRepo;
    private final BookRepo bookRepo;

    private final ObjectMapper objectMapper;

    public LibraryEventsService(LibraryEventRepo libraryEventRepo, BookRepo bookRepo, ObjectMapper objectMapper) {
        this.libraryEventRepo = libraryEventRepo;
        this.bookRepo = bookRepo;
        this.objectMapper = objectMapper;
    }

    public void process(ConsumerRecord<Integer, String> record) {
        var libraryEvent = objectMapper.readValue(record.value(), LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);

        switch(libraryEvent.getLibraryEventType()) {
            case NEW ->  save(libraryEvent);
            case UPDATE -> update(libraryEvent);
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
        validateBeforeUpdate(libraryEvent);
        libraryEventRepo.save(libraryEvent);
        log.info("Library event updated for book {}", libraryEvent.getBook());
    }

    private void validateBeforeUpdate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event must have id set when updating the event");
        }

        if (!libraryEventRepo.existsById(libraryEvent.getLibraryEventId())) {
            throw new IllegalArgumentException("Library event with id " + libraryEvent.getLibraryEventId() + " does not exist");
        }

        if (libraryEvent.getBook() == null) {
            throw new IllegalArgumentException("Library event must have book set when updating the event");
        }

        if (libraryEvent.getBook().getBookId() == null) {
            throw new IllegalArgumentException("Library event must have book id set when updating the event");
        }

        if (!bookRepo.existsById(libraryEvent.getBook().getBookId())) {
            throw new IllegalArgumentException("Book with id " + libraryEvent.getBook().getBookId() + " does not exist");
        }
    }

}
