package com.learnkafka.consumer;

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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"library-events"})
@ActiveProfiles("itest")
public class LibraryEventsConsumerFalsePositiveIT {

    // see LibraryEventsConsumerConfig#defaultErrorHandler - default retry is 3
    // see LibraryEventsConsumerConfig#errorHandler -> NotRetryableExceptions
    public static final int WANTED_NUMBER_OF_CONSUMER_RETRIES = 1;

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

    private volatile String lastValidationExceptionMessage;

    @BeforeEach
    public void setup() {
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @BeforeEach
    void installExceptionCapture() {
        doAnswer(this::captureExceptionAnswer).when(libraryEventsServiceSpy).process(isA(ConsumerRecord.class));
    }

    private Object captureExceptionAnswer(org.mockito.invocation.InvocationOnMock invocation) throws Throwable {
        try {
            return invocation.callRealMethod();
        } catch (IllegalArgumentException ex) {
            lastValidationExceptionMessage = ex.getMessage();
            throw ex;
        }
    }

    @AfterEach
    public void tearDown() {
        libraryEventRepo.deleteAll();
        lastValidationExceptionMessage = null;
    }

    @Test
    void shouldFailWhenUpdatingLibraryEventThatDoesNotExist() throws ExecutionException, InterruptedException, TimeoutException {
        int libraryEventId = 1;
        var updateEventPayload = """
                {
                    "libraryEventId": %d,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 123,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """.formatted(libraryEventId);

        shouldRetryAndKeepStorageUnchangedFor(updateEventPayload);

        assertEquals("Library event with id " + libraryEventId + " does not exist",
                lastValidationExceptionMessage);
    }

    private void shouldRetryAndKeepStorageUnchangedFor(String updateEventPayload)
            throws ExecutionException, InterruptedException, TimeoutException {
        int recordsBefore = (int) libraryEventRepo.count();
        kafkaTemplate.send(defaultTopicName, updateEventPayload).get();

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(libraryEventsConsumerSpy, times(WANTED_NUMBER_OF_CONSUMER_RETRIES)).onMessage(isA(ConsumerRecord.class));
            verify(libraryEventsServiceSpy, times(WANTED_NUMBER_OF_CONSUMER_RETRIES)).process(isA(ConsumerRecord.class));
            assertEquals(recordsBefore, libraryEventRepo.count());
            assertEquals(true, lastValidationExceptionMessage != null);
        });
    }

}
