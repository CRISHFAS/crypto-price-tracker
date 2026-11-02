package com.crishfas.crypto_price_tracker.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class PriceAnalyzerTest {

    private final PriceAnalyzer analyzer = new PriceAnalyzer();

    private static BigDecimal bd(String v) {
        return new BigDecimal(v);
    }

    @Test
    void u1_previousNoModificaElEstado() {
        assertThat(analyzer.previous("BTC")).isNull();
        assertThat(analyzer.previous("BTC")).isNull();
        analyzer.confirm("BTC", bd("100"), 1, 5);
        assertThat(analyzer.previous("BTC")).isEqualByComparingTo("100");
        assertThat(analyzer.previous("BTC")).isEqualByComparingTo("100");
    }

    @Test
    void u2_confirmaYDevuelveElPrecio() {
        analyzer.confirm("BTC", bd("110"), 1, 5);
        assertThat(analyzer.previous("BTC")).isEqualByComparingTo("110");
    }

    @Test
    void u3_simbolosIndependientes() {
        analyzer.confirm("BTC", bd("110"), 1, 5);
        assertThat(analyzer.previous("ETH")).isNull();
    }

    @Test
    void u4a_sinConfirmacionPreviaNoSeSalta() {
        assertThat(analyzer.alreadyProcessed("BTC", 1, 5)).isFalse();
    }

    @Test
    void u4b_mismaParticionOffsetIgualSeSalta() {
        analyzer.confirm("BTC", bd("100"), 1, 5);
        assertThat(analyzer.alreadyProcessed("BTC", 1, 5)).isTrue();
    }

    @Test
    void u4c_mismaParticionOffsetMenorSeSalta() {
        analyzer.confirm("BTC", bd("100"), 1, 5);
        assertThat(analyzer.alreadyProcessed("BTC", 1, 4)).isTrue();
    }

    @Test
    void u4d_mismaParticionOffsetMayorNoSeSalta() {
        analyzer.confirm("BTC", bd("100"), 1, 5);
        assertThat(analyzer.alreadyProcessed("BTC", 1, 6)).isFalse();
    }

    @Test
    void u4e_particionDistintaNoSeSalta() {
        analyzer.confirm("BTC", bd("100"), 1, 5);
        assertThat(analyzer.alreadyProcessed("BTC", 2, 3)).isFalse();
        assertThat(analyzer.alreadyProcessed("BTC", 2, 5)).isFalse();
    }

    @Test
    void u5_confirmRechazaPreciosInvalidosYNoCambiaElEstado() {
        assertThatThrownBy(() -> analyzer.confirm("BTC", bd("0"), 1, 5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> analyzer.confirm("BTC", bd("-1"), 1, 5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> analyzer.confirm("BTC", null, 1, 5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(analyzer.previous("BTC")).isNull();
    }

    @Test
    void u6a_subida() {
        assertThat(PriceAnalyzer.changePercent(bd("100"), bd("110"))).isEqualTo(bd("10.0000"));
    }

    @Test
    void u6b_bajaConRedondeo() {
        assertThat(PriceAnalyzer.changePercent(bd("110"), bd("100"))).isEqualTo(bd("-9.0909"));
    }

    @Test
    void u6c_sinCambio() {
        assertThat(PriceAnalyzer.changePercent(bd("100"), bd("100"))).isEqualTo(bd("0.0000"));
    }

    @Test
    void u6d_primerPrecioNoTieneVariacion() {
        assertThat(PriceAnalyzer.changePercent(null, bd("100"))).isNull();
    }
}
