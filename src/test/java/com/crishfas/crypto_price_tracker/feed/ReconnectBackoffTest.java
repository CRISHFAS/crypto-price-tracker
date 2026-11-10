package com.crishfas.crypto_price_tracker.feed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ReconnectBackoffTest {

    private final ReconnectBackoff backoff = ReconnectBackoff.standard();

    @Test
    void losPrimerosIntentosDuplicanLaEspera() {
        assertThat(backoff.delay(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(backoff.delay(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(backoff.delay(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(backoff.delay(4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(backoff.delay(5)).isEqualTo(Duration.ofSeconds(16));
        assertThat(backoff.delay(6)).isEqualTo(Duration.ofSeconds(32));
    }

    @Test
    void alcanzaElTopeSinPasarse() {
        assertThat(backoff.delay(7)).isEqualTo(Duration.ofSeconds(60));
        assertThat(backoff.delay(8)).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void losIntentosEnormesNoDesbordan() {
        assertThat(backoff.delay(1_000_000)).isEqualTo(Duration.ofSeconds(60));
        assertThat(backoff.delay(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void funcionaConValoresPersonalizados() {
        ReconnectBackoff corto = new ReconnectBackoff(Duration.ofMillis(500), Duration.ofSeconds(3));
        assertThat(corto.delay(1)).isEqualTo(Duration.ofMillis(500));
        assertThat(corto.delay(2)).isEqualTo(Duration.ofSeconds(1));
        assertThat(corto.delay(3)).isEqualTo(Duration.ofSeconds(2));
        assertThat(corto.delay(4)).isEqualTo(Duration.ofSeconds(3));
        assertThat(corto.delay(5)).isEqualTo(Duration.ofSeconds(3));
    }

    @Test
    void unIntentoMenorQueUnoEsInvalido() {
        for (int intento : new int[] {0, -1, Integer.MIN_VALUE}) {
            assertThatThrownBy(() -> backoff.delay(intento))
                    .as("intento=%d", intento)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void unaConfiguracionInvalidaSeRechaza() {
        assertThatThrownBy(() -> new ReconnectBackoff(Duration.ZERO, Duration.ofSeconds(60)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReconnectBackoff(Duration.ofSeconds(-1), Duration.ofSeconds(60)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ReconnectBackoff(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
