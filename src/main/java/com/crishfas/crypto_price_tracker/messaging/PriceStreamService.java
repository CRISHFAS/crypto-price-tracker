package com.crishfas.crypto_price_tracker.messaging;

import com.crishfas.crypto_price_tracker.model.PriceEvent;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** Mantiene las conexiones SSE abiertas y reenvía cada precio a todos los navegadores. */
@Service
public class PriceStreamService {

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final Map<String, PriceEvent> latest = new ConcurrentHashMap<>();

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(30 * 60 * 1000L); // el navegador se reconecta solo
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(ex -> emitters.remove(emitter));
        latest.values().forEach(price -> send(emitter, price)); // foto inicial al conectar
        return emitter;
    }

    public void broadcast(PriceEvent event) {
        latest.put(event.symbol(), event);
        emitters.forEach(emitter -> send(emitter, event));
    }

    public Collection<PriceEvent> latest() {
        return latest.values();
    }

    private void send(SseEmitter emitter, PriceEvent event) {
        try {
            emitter.send(SseEmitter.event().name("price").data(event, MediaType.APPLICATION_JSON));
        } catch (IOException | IllegalStateException ex) {
            emitters.remove(emitter); // conexión cerrada por el cliente
        }
    }
}
