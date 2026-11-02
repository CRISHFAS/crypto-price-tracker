package com.crishfas.crypto_price_tracker.processing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * Lógica pura del procesador, sin Kafka. Guarda por símbolo el último precio CONFIRMADO
 * (publicado con éxito) junto con la partición y el offset del mensaje que lo trajo.
 *
 * Límite: vive en memoria; al reiniciar se pierde (el primer precio vuelve a no tener previo
 * y un mensaje ya publicado puede reentregarse; el eventId determinista permite detectarlo).
 */
@Component
public class PriceAnalyzer {

    private static final BigDecimal HUNDRED = BigDecimal.valueOf(100);

    private record Confirmed(BigDecimal price, int partition, long offset) {}

    private final ConcurrentHashMap<String, Confirmed> confirmed = new ConcurrentHashMap<>();

    /** Último precio confirmado del símbolo, o null si no hay. No modifica el estado. */
    public BigDecimal previous(String symbol) {
        Confirmed c = confirmed.get(symbol);
        return c == null ? null : c.price();
    }

    /**
     * true si este mensaje ya se confirmó: misma partición y offset menor o igual al confirmado.
     * Los offsets son por partición, así que en otra partición no son comparables y no se salta.
     */
    public boolean alreadyProcessed(String symbol, int partition, long offset) {
        Confirmed c = confirmed.get(symbol);
        return c != null && c.partition() == partition && offset <= c.offset();
    }

    /** Llamar solo DESPUÉS de que el envío a prices.processed haya terminado con éxito. */
    public void confirm(String symbol, BigDecimal price, int partition, long offset) {
        if (price == null || price.signum() <= 0) {
            throw new IllegalArgumentException("Solo se confirman precios positivos: " + price);
        }
        confirmed.put(symbol, new Confirmed(price, partition, offset));
    }

    /** (actual - previo) / previo * 100, 4 decimales, HALF_UP. null si no hay precio previo. */
    public static BigDecimal changePercent(BigDecimal previous, BigDecimal current) {
        if (previous == null) {
            return null;
        }
        return current.subtract(previous).multiply(HUNDRED).divide(previous, 4, RoundingMode.HALF_UP);
    }
}
