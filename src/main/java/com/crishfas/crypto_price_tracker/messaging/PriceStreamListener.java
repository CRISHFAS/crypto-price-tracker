package com.crishfas.crypto_price_tracker.messaging;

import com.crishfas.crypto_price_tracker.config.KafkaConfig;
import com.crishfas.crypto_price_tracker.model.PriceEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Segundo consumer group: recibe TODOS los precios, independiente del listener de logs. */
@Component
public class PriceStreamListener {

    private final PriceStreamService stream;

    public PriceStreamListener(PriceStreamService stream) {
        this.stream = stream;
    }

    @KafkaListener(topics = KafkaConfig.TOPIC_RAW, groupId = "price-stream",
                   properties = "auto.offset.reset=latest")
    public void onPrice(PriceEvent event) {
        stream.broadcast(event);
    }
}
