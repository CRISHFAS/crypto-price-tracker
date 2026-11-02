package com.crishfas.crypto_price_tracker.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Evento de salida del procesador (versión 1).
 * eventId es determinista: prices.raw:<partición>:<offset> del mensaje de entrada,
 * así un duplicado se reconoce por el mismo identificador.
 * previousPrice y changePercent son null en el primer precio confirmado de cada símbolo.
 */
public record PriceProcessedEvent(
        String eventId,
        int version,
        String symbol,
        BigDecimal price,
        BigDecimal previousPrice,
        BigDecimal changePercent,
        Instant sourceTimestamp,
        Instant processedAt) {

    public static final int CURRENT_VERSION = 1;
}
