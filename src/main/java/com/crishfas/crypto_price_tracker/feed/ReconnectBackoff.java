package com.crishfas.crypto_price_tracker.feed;

import java.time.Duration;

/**
 * Espera antes de reconectar: se duplica en cada intento hasta un tope.
 * Con 1 s de base y 60 s de tope, el peor caso son unos 10 intentos cada 10 minutos,
 * por debajo del límite de reconexiones que Kraken aplica por IP.
 * Sin aleatoriedad: con una sola instancia no aporta nada.
 */
public class ReconnectBackoff {

    private final Duration base;
    private final Duration max;

    public ReconnectBackoff(Duration base, Duration max) {
        if (base.isZero() || base.isNegative()) {
            throw new IllegalArgumentException("La espera base debe ser positiva: " + base);
        }
        if (max.compareTo(base) < 0) {
            throw new IllegalArgumentException("El tope no puede ser menor que la base: " + max);
        }
        this.base = base;
        this.max = max;
    }

    public static ReconnectBackoff standard() {
        return new ReconnectBackoff(Duration.ofSeconds(1), Duration.ofSeconds(60));
    }

    /** Espera antes del intento número {@code attempt} (1 es el primero). */
    public Duration delay(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("El intento debe ser al menos 1: " + attempt);
        }
        Duration delay = base;
        for (int i = 1; i < attempt && delay.compareTo(max) < 0; i++) {
            delay = delay.multipliedBy(2);
        }
        return delay.compareTo(max) > 0 ? max : delay;
    }
}
