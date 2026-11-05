package com.crishfas.crypto_price_tracker;

import static org.assertj.core.api.Assertions.assertThat;

import com.crishfas.crypto_price_tracker.config.KafkaConfig;
import com.crishfas.crypto_price_tracker.model.PriceEvent;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.testcontainers.kafka.KafkaContainer;

/** Flujo completo contra un Kafka efímero: prices.raw -> price-analytics -> prices.processed. */
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = "app.simulator.enabled=false")
class PriceFlowIntegrationTest {

    @Autowired
    KafkaTemplate<String, PriceEvent> template;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Autowired
    KafkaContainer kafka;

    @Test
    void unPrecioProduceUnEventoProcesadoYElSiguienteCalculaLaVariacion() throws Exception {
        esperarAsignacion("price-analytics");
        String symbol = "FLOW-" + UUID.randomUUID().toString().substring(0, 8);

        SendResult<String, PriceEvent> first = enviar(symbol, "100");
        SendResult<String, PriceEvent> second = enviar(symbol, "110");

        List<ConsumerRecord<String, String>> events = leerProcesados(symbol, 2);

        String e1 = events.get(0).value();
        assertThat(e1).contains("\"previousPrice\":null").contains("\"changePercent\":null");
        assertThat(e1).contains("\"eventId\":\"" + eventId(first) + "\"");

        String e2 = events.get(1).value();
        assertThat(e2).contains("\"previousPrice\":100").contains("\"changePercent\":10.0000");
        assertThat(e2).contains("\"eventId\":\"" + eventId(second) + "\"");
    }

    private static String eventId(SendResult<String, PriceEvent> sent) {
        return KafkaConfig.TOPIC_RAW + ":" + sent.getRecordMetadata().partition()
                + ":" + sent.getRecordMetadata().offset();
    }

    /** price-analytics lee con 'latest': lo enviado antes de su asignación se pierde. */
    private void esperarAsignacion(String group) {
        MessageListenerContainer container = registry.getListenerContainers().stream()
                .filter(c -> group.equals(c.getGroupId()))
                .findFirst()
                .orElseThrow();
        Awaitility.await().atMost(Duration.ofSeconds(30))
                .until(() -> container.getAssignedPartitions() != null
                        && container.getAssignedPartitions().size() == 3);
    }

    private SendResult<String, PriceEvent> enviar(String symbol, String price) throws Exception {
        return template.send(KafkaConfig.TOPIC_RAW, symbol,
                new PriceEvent(symbol, new BigDecimal(price), Instant.now()))
                .get(10, TimeUnit.SECONDS);
    }

    private List<ConsumerRecord<String, String>> leerProcesados(String symbol, int esperados) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(KafkaConfig.TOPIC_PROCESSED));
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInSameThread().until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                    if (symbol.equals(r.key())) {
                        found.add(r);
                    }
                });
                return found.size() >= esperados;
            });
        }
        return found;
    }
}
