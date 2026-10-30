package com.crishfas.crypto_price_tracker.web;

import com.crishfas.crypto_price_tracker.messaging.PriceStreamService;
import com.crishfas.crypto_price_tracker.model.PriceEvent;
import java.util.Collection;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/prices")
public class PriceController {

    private final PriceStreamService stream;

    public PriceController(PriceStreamService stream) {
        this.stream = stream;
    }

    @GetMapping("/latest")
    public Collection<PriceEvent> latest() {
        return stream.latest();
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return stream.subscribe();
    }
}
