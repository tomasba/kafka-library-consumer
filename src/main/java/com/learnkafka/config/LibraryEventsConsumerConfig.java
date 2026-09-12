package com.learnkafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    private final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);

    @Bean
    public DefaultErrorHandler errorHandler() {
        List<Class<? extends RuntimeException>> nonRetryableExceptions = List.of(IllegalArgumentException.class);

        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2);
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(fixedBackOff);

        defaultErrorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Failed processing consumed library event in retry listener, attempt {}: {}", deliveryAttempt, record, ex));

        nonRetryableExceptions.forEach(defaultErrorHandler::addNotRetryableExceptions);

        return defaultErrorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        // Apply Boot's standard configuration first
        configurer.configure(factory, kafkaConsumerFactory);
        // configure multiple consumer instances in application. set to 3 as we have 3 partitions per topic
        // might be applied like this if not running in a cloud like environment (i.e. no kubernetes)
        factory.setConcurrency(3);
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler());

        return factory;
    }

}
