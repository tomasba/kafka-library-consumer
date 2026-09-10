package com.learnkafka.domain;

import jakarta.persistence.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

@Entity
@Table(name = "library_event")
public class LibraryEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer libraryEventId;

    @Enumerated(EnumType.STRING)
    private LibraryEventType libraryEventType;

    @OneToOne(mappedBy = "libraryEvent", cascade = { CascadeType.ALL })
    @NotNull
    @Valid
    private Book book;

    public LibraryEvent() {}

    public LibraryEvent(Integer libraryEventId, LibraryEventType libraryEventType, Book book) {
        this.libraryEventId = libraryEventId;
        this.libraryEventType = libraryEventType;
        this.book = book;
    }

    public Integer getLibraryEventId() { return libraryEventId; }
    public void setLibraryEventId(Integer libraryEventId) { this.libraryEventId = libraryEventId; }

    public LibraryEventType getLibraryEventType() { return libraryEventType; }
    public void setLibraryEventType(LibraryEventType libraryEventType) { this.libraryEventType = libraryEventType; }

    public Book getBook() { return book; }
    public void setBook(Book book) { this.book = book; }
}