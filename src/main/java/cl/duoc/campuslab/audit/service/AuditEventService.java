
package cl.duoc.campuslab.audit.service;

import cl.duoc.campuslab.audit.entity.AuditEvent;
import cl.duoc.campuslab.audit.repository.AuditEventRepository;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class AuditEventService {

    private final AuditEventRepository repository;

    public AuditEventService(AuditEventRepository repository) {
        this.repository = repository;
    }

    // Guardar evento recibido desde Kafka
    @Transactional
    public void saveEvent(
            String eventId,
            String eventType,
            Long bookingId,
            String status,
            LocalDateTime timestamp
    ) {
        saveEvent(
                eventId,
                eventType,
                bookingId,
                null,
                status,
                timestamp
        );
    }

    @Transactional
    public void saveEvent(
            String eventId,
            String eventType,
            Long bookingId,
            String resourceId,
            String status,
            LocalDateTime timestamp
    ) {

        // Evitar guardar el mismo evento dos veces.
        if (repository.existsByEventId(eventId)) {
            return;
        }

        AuditEvent event = new AuditEvent(
                eventId,
                eventType,
                bookingId,
                resourceId,
                status,
                timestamp
        );

        repository.saveAndFlush(event);
    }

    // Consultar todos los eventos
    @Transactional(readOnly = true)
    public List<AuditEvent> getAllEvents() {
        return repository.findAllByOrderByReceivedAtDesc();
    }

    // Consultar eventos por reserva
    @Transactional(readOnly = true)
    public List<AuditEvent> getEventsByBookingId(Long bookingId) {
        return repository.findByBookingIdOrderByReceivedAtDesc(
                bookingId
        );
    }
}
