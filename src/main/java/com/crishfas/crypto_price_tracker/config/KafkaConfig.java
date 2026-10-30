package com.crishfas.crypto_price_tracker.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class KafkaConfig {

    public static final String TOPIC_RAW = "prices.raw";

    /** Se crea al arrancar si no existe. 1 réplica porque en local hay un solo broker. */
    @Bean
    NewTopic pricesRawTopic() {
        return TopicBuilder.name(TOPIC_RAW).partitions(3).replicas(1).build();
    }
}
