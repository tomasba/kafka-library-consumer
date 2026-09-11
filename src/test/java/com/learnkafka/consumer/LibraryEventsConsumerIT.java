package com.learnkafka.consumer;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.repo.LibraryEventRepo;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"library-events"})
@ActiveProfiles("itest")
public class LibraryEventsConsumerIT {

    @Value("${spring.kafka.template.default-topic}")
    private String defaultTopicName;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private LibraryEventRepo libraryEventRepo;

    @MockitoSpyBean
    private LibraryEventsConsumer libraryEventsConsumerSpy;

    @MockitoSpyBean
    private LibraryEventsService libraryEventsServiceSpy;
    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    public void tearDown() {
        libraryEventRepo.deleteAll();
    }

    @Test
    void shouldProduceAndConsumeLibraryEventToAddNewBookToTheLibrary() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        var libraryNewBookEvent = """
                {                
                    "libraryEventId": null,
                    "libraryEventType": "NEW",
                    "book": {
                        "bookId": 123,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;
        // with .get() - blocks the calling thread until the producer has finished sending and received
        // acknowledgment from the broker. But Background processing is in progress storing message to the DB.

        // when
        var sendResult = kafkaTemplate.send(defaultTopicName, libraryNewBookEvent).get();

        // then
        // block thread for 3 seconds to wait for the consumer to process the message
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
            verify(libraryEventsServiceSpy, times(1)).process(isA(ConsumerRecord.class));


            List<LibraryEvent> libraryBookEvents = StreamSupport.stream(libraryEventRepo.findAll().spliterator(), false)
                    .toList();
            assertEquals(1, libraryBookEvents.size());
            assertNotNull(libraryBookEvents.get(0).getBook());
        });
    }

    @Test
    void shouldProduceAndConsumeLibraryEventToModifySpecifiedBookInTheLibrary() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        var libraryNewBookEvent = """
                {                
                    "libraryEventId": null,
                    "libraryEventType": "NEW",
                    "book": {
                        "bookId": 123,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;

        var sendResult = kafkaTemplate.send(defaultTopicName, libraryNewBookEvent).get();
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
            verify(libraryEventsServiceSpy, times(1)).process(isA(ConsumerRecord.class));
        });

        LibraryEvent persistedBookEvent = StreamSupport.stream(libraryEventRepo.findAll().spliterator(), false)
                .findFirst().orElseThrow();

        // when

        // no-transactional scope - so we're brave enough to alter the entity and send it to the topic again. The consumer will pick it up and update the DB.
        // another option to create the book event - just persist the book event to the DB and then send it to the topic for UPDATE. The consumer will pick it up and update the DB.
        persistedBookEvent.getBook().setBookName("Kafka Using Spring Boot - Updated");
        persistedBookEvent.setLibraryEventType(com.learnkafka.domain.LibraryEventType.UPDATE);
        kafkaTemplate.send(defaultTopicName, objectMapper.writeValueAsString(persistedBookEvent)).get();

        // then
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(libraryEventsConsumerSpy, times(2)).onMessage(isA(ConsumerRecord.class));
            verify(libraryEventsServiceSpy, times(2)).process(isA(ConsumerRecord.class));

            LibraryEvent updatedBookEvent = StreamSupport.stream(libraryEventRepo.findAll().spliterator(), false)
                    .findFirst().orElseThrow();
            assertEquals("Kafka Using Spring Boot - Updated", updatedBookEvent.getBook().getBookName());
        });

    }

}
