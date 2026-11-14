
package cl.duoc.campuslab.audit.kafka;

import cl.duoc.campuslab.audit.dto.BookingKafkaEvent;
import cl.duoc.campuslab.audit.service.AuditEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class AuditKafkaConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(AuditKafkaConsumer.class);

    private final AuditEventService auditEventService;
    private final ObjectMapper objectMapper;

    public AuditKafkaConsumer(
            AuditEventService auditEventService,
            ObjectMapper objectMapper
    ) {
        this.auditEventService = auditEventService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${campuslab.kafka.topics.booking-events}",
            groupId = "campuslab-audit-group"
    )
    public void consume(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment
    ) throws Exception {

        // 1. Convertir el JSON recibido a nuestro DTO.
        BookingKafkaEvent event = objectMapper.readValue(
                record.value(),
                BookingKafkaEvent.class
        );

        // 2. Comprobar que el evento tenga sus datos básicos.
        if (event.eventId() == null ||
                event.eventId().isBlank() ||
                event.eventType() == null ||
                event.eventType().isBlank() ||
                event.bookingId() == null ||
                event.timestamp() == null) {

            throw new IllegalArgumentException(
                    "Evento Kafka incompleto"
            );
        }

        // 3. Guardar el evento en H2.
        auditEventService.saveEvent(
                event.eventId(),
                event.eventType(),
                event.bookingId(),
                event.status(),
                event.timestamp()
        );

        // 4. Confirmar el offset después de guardar.
        acknowledgment.acknowledge();

        log.info(
                "AUDITORIA KAFKA: Evento {} | Reserva {} | Particion {} | Offset {}",
                event.eventType(),
                event.bookingId(),
                record.partition(),
                record.offset()
        );
    }
}
