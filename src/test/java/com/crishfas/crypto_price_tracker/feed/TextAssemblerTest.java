package com.crishfas.crypto_price_tracker.feed;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TextAssemblerTest {

    @Test
    void unTrozoUnicoDevuelveElMensaje() {
        assertThat(new TextAssembler().add("{\"a\":1}", true)).contains("{\"a\":1}");
    }

    @Test
    void variosTrozosSoloDevuelvenElMensajeAlLlegarElUltimo() {
        TextAssembler assembler = new TextAssembler();
        assertThat(assembler.add("{\"a\":", false)).isEmpty();
        assertThat(assembler.add("[1,", false)).isEmpty();
        assertThat(assembler.add("2]}", true)).contains("{\"a\":[1,2]}");
    }

    @Test
    void trasUnMensajeCompletoElSiguienteEmpiezaLimpio() {
        TextAssembler assembler = new TextAssembler();
        assertThat(assembler.add("uno", true)).contains("uno");
        assertThat(assembler.add("dos", true)).contains("dos");
    }

    @Test
    void unMensajeVacioDevuelveUnaCadenaVacia() {
        assertThat(new TextAssembler().add("", true)).contains("");
    }

    @Test
    void unMensajeMayorQueElLimiteSeDescartaEnteroYElSiguienteSeRecibeBien() {
        TextAssembler assembler = new TextAssembler(10);
        assertThat(assembler.add("0123456789", false)).isEmpty();
        assertThat(assembler.add("1", true)).isEmpty();
        assertThat(assembler.add("{}", true)).contains("{}");
    }
}
