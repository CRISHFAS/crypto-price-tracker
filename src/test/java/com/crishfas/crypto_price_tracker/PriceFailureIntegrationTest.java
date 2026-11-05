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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

/** Fallos contra un Kafka efímero: precio negativo y mensaje que no es JSON. */
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = "app.simulator.enabled=false")
class PriceFailureIntegrationTest {

    private static final String GRUPO = "kafka_dlt-original-consumer-group";
    private static final String EXCEPCION = "kafka_dlt-exception-fqcn";

    @Autowired
    KafkaTemplate<String, PriceEvent> template;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Autowired
    KafkaContainer kafka;

    @Test
    void unPrecioNegativoVaAlDltYNoContaminaElEstado() throws Exception {
        esperarAsignacion("price-analytics");
        String symbol = "NEG-" + UUID.randomUUID().toString().substring(0, 8);

        enviar(symbol, "-5");
        SendResult<String, PriceEvent> valido = enviar(symbol, "100");

        List<ConsumerRecord<String, String>> procesados = leer(KafkaConfig.TOPIC_PROCESSED, symbol, 1);
        assertThat(procesados).hasSize(1);
        assertThat(procesados.get(0).value())
                .contains("\"previousPrice\":null")
                .contains("\"eventId\":\"" + eventId(valido) + "\"");

        List<ConsumerRecord<String, String>> dlt = leer(KafkaConfig.TOPIC_RAW_DLT, symbol, 1);
        assertThat(dlt).hasSize(1);
        assertThat(cabecera(dlt.get(0), GRUPO)).isEqualTo("price-processor");
        assertThat(dlt.get(0).value()).contains("\"price\":-5");
    }

    @Test
    void unMensajeIlegibleVaAlDltUnaVezPorGrupoYElFlujoSigue() throws Exception {
        esperarAsignacion("price-analytics");
        esperarAsignacion("price-stream");
        String symbol = "BAD-" + UUID.randomUUID().toString().substring(0, 8);

        enviarTexto(symbol, "esto no es json");
        SendResult<String, PriceEvent> valido = enviar(symbol, "100");

        List<ConsumerRecord<String, String>> dlt = leer(KafkaConfig.TOPIC_RAW_DLT, symbol, 3);
        assertThat(dlt).hasSize(3);
        assertThat(dlt).extracting(r -> r.value()).containsOnly("esto no es json");
        assertThat(dlt).extracting(r -> cabecera(r, GRUPO))
                .containsExactlyInAnyOrder("price-processor", "price-analytics", "price-stream");
        assertThat(dlt).allSatisfy(r ->
                assertThat(cabecera(r, EXCEPCION)).endsWith("DeserializationException"));

        List<ConsumerRecord<String, String>> procesados = leer(KafkaConfig.TOPIC_PROCESSED, symbol, 1);
        assertThat(procesados).hasSize(1);
        assertThat(procesados.get(0).value())
                .contains("\"previousPrice\":null")
                .contains("\"eventId\":\"" + eventId(valido) + "\"");
    }

    private static String eventId(SendResult<String, PriceEvent> sent) {
        return KafkaConfig.TOPIC_RAW + ":" + sent.getRecordMetadata().partition()
                + ":" + sent.getRecordMetadata().offset();
    }

    private static String cabecera(ConsumerRecord<String, String> r, String nombre) {
        Header h = r.headers().lastHeader(nombre);
        return h == null ? null : new String(h.value());
    }

    /** Los grupos con 'latest' pierden lo enviado antes de su asignación. */
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

    private void enviarTexto(String symbol, String texto) throws Exception {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(KafkaConfig.TOPIC_RAW, symbol, texto))
                    .get(10, TimeUnit.SECONDS);
        }
    }

    /** Lee hasta tener 'minimo' registros de la clave y sigue unos 2 s por si llegan más. */
    private List<ConsumerRecord<String, String>> leer(String topic, String key, int minimo) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInSameThread().until(() -> {
                recoger(consumer, key, found);
                return found.size() >= minimo;
            });
            for (int i = 0; i < 10; i++) {
                recoger(consumer, key, found);
            }
        }
        return found;
    }

    private static void recoger(KafkaConsumer<String, String> consumer, String key,
                                List<ConsumerRecord<String, String>> found) {
        consumer.poll(Duration.ofMillis(200)).forEach(r -> {
            if (key.equals(r.key())) {
                found.add(r);
            }
        });
    }
}
