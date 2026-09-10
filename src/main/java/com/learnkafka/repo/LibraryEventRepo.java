package com.learnkafka.repo;

import com.learnkafka.domain.LibraryEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LibraryEventRepo extends CrudRepository<LibraryEvent, Integer> {
}