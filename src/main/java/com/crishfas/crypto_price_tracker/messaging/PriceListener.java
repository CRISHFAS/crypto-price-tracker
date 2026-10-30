package com.crishfas.crypto_price_tracker.messaging;

import com.crishfas.crypto_price_tracker.config.KafkaConfig;
import com.crishfas.crypto_price_tracker.model.PriceEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PriceListener {

    private static final Logger log = LoggerFactory.getLogger(PriceListener.class);

    @KafkaListener(topics = KafkaConfig.TOPIC_RAW, groupId = "price-processor")
    public void onPrice(ConsumerRecord<String, PriceEvent> record) {
        PriceEvent e = record.value();
        log.info("[partición {} | offset {}] {} = {}", record.partition(), record.offset(), e.symbol(), e.price());
    }
}
