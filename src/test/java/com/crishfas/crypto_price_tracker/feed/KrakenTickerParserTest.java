package com.crishfas.crypto_price_tracker.feed;

import static org.assertj.core.api.Assertions.assertThat;

import com.crishfas.crypto_price_tracker.model.PriceEvent;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class KrakenTickerParserTest {

    private static final Instant AHORA = Instant.parse("2026-01-01T00:00:00Z");

    // Mensajes REALES capturados de wss://ws.kraken.com/v2 el 09/11/2026.
    private static final String SNAPSHOT_SOL = """
            {"channel":"ticker","type":"snapshot","data":[{"symbol":"SOL/USD","bid":87.14,"bid_qty":163.00490809,"ask":87.16,"ask_qty":121.36021808,"last":87.17,"volume":279884.99894333,"vwap":87.77,"low":85.00,"high":89.88,"change":-2.39,"change_pct":-2.67,"trades":30207,"timestamp":"2026-11-09T00:17:06.146873Z"}]}""";
    private static final String UPDATE_ETH = """
            {"channel":"ticker","type":"update","data":[{"symbol":"ETH/USD","bid":2137.80,"bid_qty":4.42309407,"ask":2137.99,"ask_qty":0.01403187,"last":2137.80,"volume":25355.10174497,"vwap":2169.42,"low":2071.00,"high":2242.86,"change":-97.33,"change_pct":-4.35,"trades":39550,"timestamp":"2026-11-09T00:17:06.993099Z"}]}""";
    private static final String ACUSE_BTC = """
            {"method":"subscribe","result":{"channel":"ticker","event_trigger":"trades","snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2026-11-09T00:17:06.441804Z","time_out":"2026-11-09T00:17:06.441869Z"}""";

    // RECONSTRUIDO a partir del inicio visible del mensaje real (el resto se truncó en la captura).
    private static final String ESTADO = """
            {"channel":"status","type":"update","data":[{"version":"2.0.10","system":"online","api_version":"v2","connection_id":18103441642533705793}]}""";

    private final KrakenTickerParser parser =
            new KrakenTickerParser(Clock.fixed(AHORA, ZoneOffset.UTC));

    /** SINTÉTICO: un ticker mínimo con el símbolo y el precio indicados. */
    private static String ticker(String simbolo, String ultimo) {
        return """
                {"channel":"ticker","type":"update","data":[{"symbol":"%s","last":%s}]}"""
                .formatted(simbolo, ultimo);
    }

    @Test
    void snapshotRealDeSolDevuelveUnEvento() {
        List<PriceEvent> events = parser.parse(SNAPSHOT_SOL);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).symbol()).isEqualTo("SOL");
        assertThat(events.get(0).price()).isEqualByComparingTo("87.17");
        assertThat(events.get(0).timestamp()).isEqualTo(Instant.parse("2026-11-09T00:17:06.146873Z"));
    }

    @Test
    void updateRealDeEthConservaLaEscalaDelPrecio() {
        List<PriceEvent> events = parser.parse(UPDATE_ETH);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).symbol()).isEqualTo("ETH");
        assertThat(events.get(0).price().toPlainString()).isEqualTo("2137.80");
    }

    @Test
    void acuseRealDeSuscripcionNoDevuelveNada() {
        assertThat(parser.parse(ACUSE_BTC)).isEmpty();
    }

    @Test
    void mensajeDeEstadoNoDevuelveNada() {
        assertThat(parser.parse(ESTADO)).isEmpty();
    }

    @Test
    void sinPrecioLastNoDevuelveNada() {
        String json = """
                {"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":75001.2,"ask":75001.3}]}""";
        assertThat(parser.parse(json)).isEmpty();
    }

    @Test
    void lastCeroNegativoNuloOTextoNoDevuelveNada() {
        for (String ultimo : List.of("0", "0.00", "-5", "null", "\"75000\"")) {
            assertThat(parser.parse(ticker("BTC/USD", ultimo))).as("last=%s", ultimo).isEmpty();
        }
    }

    @Test
    void mensajesIlegiblesOIncompletosNoDevuelvenNada() {
        for (String texto : List.of("esto no es json", "", "{", "[]", "null", "42")) {
            assertThat(parser.parse(texto)).as("texto='%s'", texto).isEmpty();
        }
    }

    @Test
    void canalesQueNoSonTickerNoDevuelvenNada() {
        assertThat(parser.parse("{\"channel\":\"heartbeat\"}")).isEmpty();
        String json = """
                {"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","last":75000.0}]}""";
        assertThat(parser.parse(json)).isEmpty();
    }

    @Test
    void simbolosSinBarraOEnOtraMonedaNoDevuelvenNada() {
        for (String simbolo : List.of("BTCUSD", "BTC/EUR", "/USD", "")) {
            assertThat(parser.parse(ticker(simbolo, "75000.5"))).as("symbol=%s", simbolo).isEmpty();
        }
    }

    @Test
    void sinTimestampUsaElReloj() {
        List<PriceEvent> events = parser.parse(ticker("BTC/USD", "75000.5"));
        assertThat(events).hasSize(1);
        assertThat(events.get(0).timestamp()).isEqualTo(AHORA);
    }

    @Test
    void timestampIlegibleUsaElReloj() {
        String json = """
                {"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","last":75000.5,"timestamp":"ayer"}]}""";
        List<PriceEvent> events = parser.parse(json);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).timestamp()).isEqualTo(AHORA);
    }

    @Test
    void variosElementosDevuelvenUnEventoCadaUnoEnOrden() {
        String json = """
                {"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","last":75000.5},{"symbol":"ETH/USD","last":2137.8}]}""";
        assertThat(parser.parse(json)).extracting(PriceEvent::symbol).containsExactly("BTC", "ETH");
    }

    @Test
    void dataQueNoEsUnaListaNoDevuelveNada() {
        assertThat(parser.parse("{\"channel\":\"ticker\",\"data\":{\"symbol\":\"BTC/USD\",\"last\":1}}"))
                .isEmpty();
    }
}
