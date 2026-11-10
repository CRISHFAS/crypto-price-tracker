package com.crishfas.crypto_price_tracker.feed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.crishfas.crypto_price_tracker.messaging.PricePublisher;
import com.crishfas.crypto_price_tracker.model.PriceEvent;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class KrakenPriceFeedTest {

    // Mensaje REAL capturado de wss://ws.kraken.com/v2 el 09/11/2026.
    private static final String UPDATE_ETH = """
            {"channel":"ticker","type":"update","data":[{"symbol":"ETH/USD","bid":2137.80,"bid_qty":4.42309407,"ask":2137.99,"ask_qty":0.01403187,"last":2137.80,"volume":25355.10174497,"vwap":2169.42,"low":2071.00,"high":2242.86,"change":-97.33,"change_pct":-4.35,"trades":39550,"timestamp":"2026-11-09T00:17:06.993099Z"}]}""";

    // RECONSTRUIDO a partir del inicio visible del mensaje real (la captura lo truncó).
    private static final String ESTADO = """
            {"channel":"status","type":"update","data":[{"version":"2.0.10","system":"online","api_version":"v2","connection_id":18103441642533705793}]}""";

    private final List<PriceEvent> publicados = new ArrayList<>();

    private final PricePublisher publicadorDePrueba = new PricePublisher(null) {
        @Override
        public void publish(PriceEvent event) {
            publicados.add(event);
        }
    };

    private KrakenPriceFeed feed() {
        return new KrakenPriceFeed(publicadorDePrueba, "wss://ws.kraken.com/v2", "BTC/USD, ETH/USD ,SOL/USD");
    }

    @Test
    void laSuscripcionPideElCanalTickerConLosTresSimbolos() {
        assertThat(KrakenPriceFeed.subscribeMessage(KrakenPriceFeed.parseSymbols("BTC/USD, ETH/USD ,SOL/USD")))
                .isEqualTo("{\"method\":\"subscribe\",\"params\":{\"channel\":\"ticker\","
                        + "\"symbol\":[\"BTC/USD\",\"ETH/USD\",\"SOL/USD\"]}}");
    }

    @Test
    void losSimbolosInvalidosSeRechazan() {
        for (String texto : List.of("btc/usd", "BTC\"/USD", "BTCUSD", "BTC/USD,X\"]}", "BTC/")) {
            assertThatThrownBy(() -> KrakenPriceFeed.parseSymbols(texto))
                    .as("texto=%s", texto)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void unaListaSinSimbolosSeRechaza() {
        for (String texto : List.of("", " , ,")) {
            assertThatThrownBy(() -> KrakenPriceFeed.parseSymbols(texto))
                    .as("texto='%s'", texto)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void unTickerRealPublicaUnEventoYDevuelveUno() {
        assertThat(feed().handleMessage(UPDATE_ETH)).isEqualTo(1);
        assertThat(publicados).hasSize(1);
        assertThat(publicados.get(0).symbol()).isEqualTo("ETH");
        assertThat(publicados.get(0).price()).isEqualByComparingTo("2137.80");
    }

    @Test
    void elMensajeDeEstadoNoPublicaNada() {
        assertThat(feed().handleMessage(ESTADO)).isZero();
        assertThat(publicados).isEmpty();
    }
}
