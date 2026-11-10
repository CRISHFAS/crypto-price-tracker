package com.crishfas.crypto_price_tracker.feed;

import com.crishfas.crypto_price_tracker.messaging.PricePublisher;
import com.crishfas.crypto_price_tracker.model.PriceEvent;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Precios reales del canal ticker de Kraken (WebSocket v2), publicados en prices.raw.
 * Apagado por defecto: el simulador sigue siendo el origen predeterminado.
 * Reconecta con espera creciente y, si pasan 60 s sin ningún mensaje, fuerza la reconexión.
 * Cada conexión lleva un número de generación: lo que llega de una conexión ya sustituida se ignora.
 */
@Component
@ConditionalOnProperty(name = "app.kraken.enabled", havingValue = "true")
public class KrakenPriceFeed {

    private static final Logger log = LoggerFactory.getLogger(KrakenPriceFeed.class);
    private static final Pattern SYMBOL = Pattern.compile("[A-Z0-9]{2,10}/[A-Z]{3,5}");
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(15);
    private static final long SILENCE_LIMIT_NANOS = Duration.ofSeconds(60).toNanos();

    private final PricePublisher publisher;
    private final String url;
    private final List<String> symbols;
    private final KrakenTickerParser parser = new KrakenTickerParser(Clock.systemUTC());
    private final ReconnectBackoff backoff = ReconnectBackoff.standard();
    private final HttpClient http = HttpClient.newHttpClient();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "kraken-feed");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicInteger generation = new AtomicInteger();
    private final AtomicInteger attempt = new AtomicInteger();

    private volatile boolean running;
    private volatile boolean connected;
    private volatile long lastMessageNanos;
    private volatile WebSocket current;

    public KrakenPriceFeed(PricePublisher publisher,
                           @Value("${app.kraken.url:wss://ws.kraken.com/v2}") String url,
                           @Value("${app.kraken.symbols:BTC/USD,ETH/USD,SOL/USD}") String symbols) {
        this.publisher = publisher;
        this.url = url;
        this.symbols = parseSymbols(symbols);
    }

    /** Los símbolos se insertan en un texto JSON: se validan para que no puedan romperlo. */
    static List<String> parseSymbols(String text) {
        List<String> result = new ArrayList<>();
        for (String part : text.split(",")) {
            String symbol = part.trim();
            if (symbol.isEmpty()) {
                continue;
            }
            if (!SYMBOL.matcher(symbol).matches()) {
                throw new IllegalArgumentException("Símbolo no válido: " + symbol);
            }
            result.add(symbol);
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("Hace falta al menos un símbolo");
        }
        return List.copyOf(result);
    }

    static String subscribeMessage(List<String> symbols) {
        String list = symbols.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));
        return "{\"method\":\"subscribe\",\"params\":{\"channel\":\"ticker\",\"symbol\":[" + list + "]}}";
    }

    /** Analiza un mensaje completo y publica sus precios. Devuelve cuántos publicó. */
    int handleMessage(String message) {
        List<PriceEvent> events = parser.parse(message);
        events.forEach(publisher::publish);
        return events.size();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        running = true;
        scheduler.scheduleWithFixedDelay(this::checkSilence, 10, 10, TimeUnit.SECONDS);
        connect();
    }

    @PreDestroy
    public void stop() {
        running = false;
        scheduler.shutdownNow();
        WebSocket ws = current;
        if (ws != null) {
            ws.abort();
        }
    }

    private synchronized void connect() {
        if (!running) {
            return;
        }
        int id = generation.incrementAndGet();
        log.info("[kraken] conectando a {}", url);
        http.newWebSocketBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .buildAsync(URI.create(url), new Connection(id))
                .whenComplete((ws, error) -> {
                    if (error != null) {
                        log.warn("[kraken] no se pudo conectar: {}", error.toString());
                        reconnect(id);
                    } else if (id == generation.get()) {
                        current = ws;
                    } else {
                        ws.abort();
                    }
                });
    }

    private synchronized void reconnect(int fromGeneration) {
        if (!running || fromGeneration != generation.get()) {
            return;
        }
        generation.incrementAndGet();
        connected = false;
        WebSocket old = current;
        current = null;
        if (old != null) {
            old.abort();
        }
        int n = attempt.incrementAndGet();
        Duration wait = backoff.delay(n);
        log.warn("[kraken] reconectando en {} (intento {})", wait, n);
        try {
            scheduler.schedule(this::connect, wait.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // la aplicación se está cerrando
        }
    }

    private void checkSilence() {
        if (connected && System.nanoTime() - lastMessageNanos > SILENCE_LIMIT_NANOS) {
            log.warn("[kraken] sin mensajes desde hace más de 60 s: se reconecta");
            reconnect(generation.get());
        }
    }

    private final class Connection implements WebSocket.Listener {

        private final int id;
        private final TextAssembler assembler = new TextAssembler();

        Connection(int id) {
            this.id = id;
        }

        private boolean stale() {
            return id != generation.get();
        }

        @Override
        public void onOpen(WebSocket ws) {
            if (stale()) {
                ws.abort();
                return;
            }
            log.info("[kraken] conexión abierta; suscribiendo a {}", symbols);
            connected = true;
            lastMessageNanos = System.nanoTime();
            ws.sendText(subscribeMessage(symbols), true);
            ws.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            ws.request(1);
            if (stale()) {
                return null;
            }
            lastMessageNanos = System.nanoTime();
            try {
                assembler.add(data, last).ifPresent(message -> {
                    if (handleMessage(message) > 0) {
                        attempt.set(0);
                    }
                });
            } catch (RuntimeException e) {
                log.warn("[kraken] mensaje descartado por un error inesperado", e);
            }
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
            log.warn("[kraken] conexión cerrada: {} {}", code, reason);
            reconnect(id);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            log.warn("[kraken] error en la conexión: {}", error.toString());
            reconnect(id);
        }
    }
}
