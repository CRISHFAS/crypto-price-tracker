package com.crishfas.crypto_price_tracker.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableScheduling
public class KafkaConfig {

    public static final String TOPIC_RAW = "prices.raw";
    public static final String TOPIC_RAW_DLT = "prices.raw-dlt";

    /** Se crea al arrancar si no existe. 1 réplica porque en local hay un solo broker. */
    @Bean
    NewTopic pricesRawTopic() {
        return TopicBuilder.name(TOPIC_RAW).partitions(3).replicas(1).build();
    }

    /** Debe tener al menos tantas particiones como el original: el mensaje fallido va a la misma partición. */
    @Bean
    NewTopic pricesRawDltTopic() {
        return TopicBuilder.name(TOPIC_RAW_DLT).partitions(3).replicas(1).build();
    }

    /**
     * Si un listener lanza una excepción: reintenta 3 veces más (4 intentos) con 1 s de espera
     * y, si sigue fallando, publica el mensaje en <topic>-dlt y continúa con el siguiente.
     * Spring Boot aplica este bean a todos los @KafkaListener.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
    }
}
