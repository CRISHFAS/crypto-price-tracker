package com.crishfas.crypto_price_tracker.messaging;

import com.crishfas.crypto_price_tracker.config.KafkaConfig;
import com.crishfas.crypto_price_tracker.model.PriceEvent;
import com.crishfas.crypto_price_tracker.model.PriceProcessedEvent;
import com.crishfas.crypto_price_tracker.processing.PriceAnalyzer;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Tercer consumer group: calcula la variación porcentual y publica en prices.processed.
 * Orden: previous() -> enviar y esperar -> confirm(). Si el envío falla se lanza una
 * IllegalStateException (reintentable), el DefaultErrorHandler reintenta y, agotados los
 * intentos, el mensaje va a prices.raw-dlt. El estado solo avanza tras un envío correcto.
 */
@Component
public class PriceAnalyticsListener {

    private static final Logger log = LoggerFactory.getLogger(PriceAnalyticsListener.class);
    private static final String GROUP = "price-analytics";
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final PriceAnalyzer analyzer;
    private final KafkaTemplate<String, PriceProcessedEvent> kafka;

    public PriceAnalyticsListener(PriceAnalyzer analyzer, KafkaTemplate<String, PriceProcessedEvent> kafka) {
        this.analyzer = analyzer;
        this.kafka = kafka;
    }

    @KafkaListener(topics = KafkaConfig.TOPIC_RAW, groupId = GROUP,
                   properties = "auto.offset.reset=latest")
    public void onPrice(ConsumerRecord<String, PriceEvent> record) {
        PriceEvent in = record.value();
        String eventId = KafkaConfig.TOPIC_RAW + ":" + record.partition() + ":" + record.offset();

        log.info("[{}] intento eventId={} symbol={} price={}", GROUP, eventId,
                in == null ? null : in.symbol(), in == null ? null : in.price());

        if (in == null || in.price() == null || in.price().signum() <= 0) {
            log.info("[{}] descartado (precio no positivo) eventId={}", GROUP, eventId);
            return; // el DLT de estos mensajes lo gestiona el grupo price-processor
        }
        if (analyzer.alreadyProcessed(in.symbol(), record.partition(), record.offset())) {
            log.info("[{}] ya procesado, se salta eventId={}", GROUP, eventId);
            return;
        }

        BigDecimal previous = analyzer.previous(in.symbol());
        BigDecimal change = PriceAnalyzer.changePercent(previous, in.price());
        var out = new PriceProcessedEvent(eventId, PriceProcessedEvent.CURRENT_VERSION, in.symbol(),
                in.price(), previous, change, in.timestamp(), Instant.now());

        try {
            kafka.send(KafkaConfig.TOPIC_PROCESSED, in.symbol(), out)
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrumpido al publicar " + eventId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("No se pudo publicar " + eventId
                    + " en " + KafkaConfig.TOPIC_PROCESSED, e);
        }

        analyzer.confirm(in.symbol(), in.price(), record.partition(), record.offset());
        log.info("[{}] publicado eventId={} change={}", GROUP, eventId, change);
    }
}
