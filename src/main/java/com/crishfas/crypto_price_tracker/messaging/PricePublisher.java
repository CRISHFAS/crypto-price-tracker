package com.crishfas.crypto_price_tracker.messaging;

import com.crishfas.crypto_price_tracker.config.KafkaConfig;
import com.crishfas.crypto_price_tracker.model.PriceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PricePublisher {

    private static final Logger log = LoggerFactory.getLogger(PricePublisher.class);
    private final KafkaTemplate<String, PriceEvent> kafka;

    public PricePublisher(KafkaTemplate<String, PriceEvent> kafka) {
        this.kafka = kafka;
    }

    /** La clave es el símbolo: todos los precios de BTC van a la misma partición y conservan el orden. */
    public void publish(PriceEvent event) {
        kafka.send(KafkaConfig.TOPIC_RAW, event.symbol(), event)
             .whenComplete((result, ex) -> {
                 if (ex != null) {
                     log.error("No se pudo publicar {}", event, ex);
                 }
             });
    }
}
