package com.crishfas.crypto_price_tracker.model;

import java.math.BigDecimal;
import java.time.Instant;

/** Evento inmutable: el precio de un símbolo en un instante. */
public record PriceEvent(String symbol, BigDecimal price, Instant timestamp) {}
