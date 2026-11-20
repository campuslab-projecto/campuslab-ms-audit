
package cl.duoc.campuslab.audit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "campuslab.kafka.topics.booking-events=campuslab.demo.ep5.dlt.test",
                "spring.kafka.consumer.group-id=campuslab-ep5-test-group",
                "spring.datasource.url=jdbc:h2:mem:ep5audit;DB_CLOSE_DELAY=-1",
                "spring.jpa.hibernate.ddl-auto=create-drop"
        }
)
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "campuslab.demo.ep5.dlt.test",
                "campuslab.demo.ep5.dlt.test.DLT"
        },
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KafkaDltIntegrationTest {

    private static final String TOPIC =
            "campuslab.demo.ep5.dlt.test";

    private static final String DLT =
            "campuslab.demo.ep5.dlt.test.DLT";

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void eventoInvalidoDebeTerminarEnDLT() throws Exception {

        String eventoInvalido = "{}";

        Properties properties = new Properties();

        properties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                embeddedKafka.getBrokersAsString()
        );

        properties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "campuslab-ep5-dlt-verification"
        );

        properties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        properties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        properties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );

        properties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false"
        );

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(properties)) {

            TopicPartition partition =
                    new TopicPartition(DLT, 0);

            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));

            System.out.println(
                    "EP5: Enviando evento invalido a " + TOPIC
            );

            kafkaTemplate.send(
                    TOPIC,
                    "ep5-test",
                    eventoInvalido
            ).get(15, TimeUnit.SECONDS);

            ConsumerRecord<String, String> resultado = null;

            long limite = System.currentTimeMillis() + 30000;

            while (System.currentTimeMillis() < limite
                    && resultado == null) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    resultado = record;
                    break;
                }
            }

            assertNotNull(
                    resultado,
                    "El evento invalido no llego al DLT"
            );

            assertEquals(DLT, resultado.topic());

            assertEquals(
                    eventoInvalido,
                    resultado.value()
            );

            System.out.println(
                    "EP5: PRUEBA EXITOSA - Evento recibido en DLT"
            );

            System.out.println(
                    "EP5: Topico destino = " + resultado.topic()
            );
        }
    }
}
