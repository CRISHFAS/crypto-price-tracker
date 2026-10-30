package com.crishfas.crypto_price_tracker.messaging;

import com.crishfas.crypto_price_tracker.model.PriceEvent;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Genera precios simulados cada 2 s. Más adelante se reemplaza por una API real. */
@Component
@ConditionalOnProperty(name = "app.simulator.enabled", havingValue = "true")
public class SimulatedPriceProducer {

    private final PricePublisher publisher;
    private final Map<String, BigDecimal> prices = new LinkedHashMap<>();

    public SimulatedPriceProducer(PricePublisher publisher) {
        this.publisher = publisher;
        prices.put("BTC", new BigDecimal("67000.00"));
        prices.put("ETH", new BigDecimal("3400.00"));
        prices.put("SOL", new BigDecimal("150.00"));
    }

    @Scheduled(fixedRate = 2000)
    public void tick() {
        prices.replaceAll((symbol, price) -> {
            double factor = 1 + (ThreadLocalRandom.current().nextDouble() - 0.5) / 100; // ±0,5 %
            return price.multiply(BigDecimal.valueOf(factor)).setScale(2, RoundingMode.HALF_UP);
        });
        prices.forEach((symbol, price) ->
            publisher.publish(new PriceEvent(symbol, price, Instant.now())));
    }
}
