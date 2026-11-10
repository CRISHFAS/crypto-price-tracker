package com.crishfas.crypto_price_tracker.feed;

import java.util.Optional;

/**
 * Junta los trozos de un mensaje WebSocket (el cliente del JDK puede entregar uno en varias
 * partes, con last=false salvo en la última). Un mensaje mayor que el límite se descarta
 * entero, para que un servidor defectuoso no llene la memoria.
 */
final class TextAssembler {

    private final int maxLength;
    private final StringBuilder buffer = new StringBuilder();
    private boolean overflow;

    TextAssembler() {
        this(1_000_000);
    }

    TextAssembler(int maxLength) {
        this.maxLength = maxLength;
    }

    /** Devuelve el mensaje completo al llegar el último trozo; vacío mientras falten o si se descartó. */
    Optional<String> add(CharSequence part, boolean last) {
        if (!overflow) {
            if (buffer.length() + part.length() > maxLength) {
                overflow = true;
                buffer.setLength(0);
            } else {
                buffer.append(part);
            }
        }
        if (!last) {
            return Optional.empty();
        }
        boolean discard = overflow;
        String message = buffer.toString();
        buffer.setLength(0);
        overflow = false;
        return discard ? Optional.empty() : Optional.of(message);
    }
}
