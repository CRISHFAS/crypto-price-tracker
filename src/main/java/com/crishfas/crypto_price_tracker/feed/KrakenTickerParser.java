package com.crishfas.crypto_price_tracker.feed;

import com.crishfas.crypto_price_tracker.model.PriceEvent;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Convierte mensajes del canal ticker de Kraken (WebSocket v2) en PriceEvent.
 * Lógica pura, sin red: devuelve una lista vacía para todo lo que no sea un ticker válido
 * (estado, acuses, latidos, JSON ilegible, precios ausentes o no positivos).
 * Solo se aceptan pares con cotización en USD (BTC/USD pasa a BTC).
 */
public class KrakenTickerParser {

    private static final String QUOTE = "/USD";

    private final JsonMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();
    private final Clock clock;

    public KrakenTickerParser(Clock clock) {
        this.clock = clock;
    }

    public List<PriceEvent> parse(String message) {
        JsonNode root;
        try {
            root = mapper.readTree(message);
        } catch (RuntimeException e) {
            return List.of();
        }
        if (root == null) {
            return List.of();
        }
        JsonNode channel = root.path("channel");
        JsonNode data = root.path("data");
        if (!channel.isString() || !"ticker".equals(channel.stringValue()) || !data.isArray()) {
            return List.of();
        }
        List<PriceEvent> events = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            toEvent(data.get(i)).ifPresent(events::add);
        }
        return events;
    }

    private Optional<PriceEvent> toEvent(JsonNode item) {
        JsonNode pair = item.path("symbol");
        JsonNode last = item.path("last");
        if (!pair.isString() || !last.isNumber()) {
            return Optional.empty();
        }
        String name = pair.stringValue();
        if (!name.endsWith(QUOTE) || name.length() == QUOTE.length()) {
            return Optional.empty();
        }
        BigDecimal price = last.decimalValue();
        if (price.signum() <= 0) {
            return Optional.empty();
        }
        String symbol = name.substring(0, name.length() - QUOTE.length());
        return Optional.of(new PriceEvent(symbol, price, timestamp(item.path("timestamp"))));
    }

    private Instant timestamp(JsonNode node) {
        if (node.isString()) {
            try {
                return Instant.parse(node.stringValue());
            } catch (DateTimeParseException e) {
                // sin hora válida en el dato: se usa la del reloj
            }
        }
        return clock.instant();
    }
}
