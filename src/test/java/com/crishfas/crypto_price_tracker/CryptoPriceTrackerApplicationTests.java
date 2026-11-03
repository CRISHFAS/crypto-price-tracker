package com.crishfas.crypto_price_tracker;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/** Arranca la app completa contra un Kafka efímero (Testcontainers), no contra localhost:9092. */
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = "app.simulator.enabled=false")
class CryptoPriceTrackerApplicationTests {

    @Test
    void contextLoads() {
    }

}
