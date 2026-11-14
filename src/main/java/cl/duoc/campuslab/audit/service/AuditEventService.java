
package cl.duoc.campuslab.audit.service;

import cl.duoc.campuslab.audit.entity.AuditEvent;
import cl.duoc.campuslab.audit.repository.AuditEventRepository;

import org.springframework.dao.DataIntegrityViolationException;
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

    // ============================================
    // 1. GUARDAR EVENTO RECIBIDO DESDE KAFKA
    // ============================================

    @Transactional
    public void saveEvent(
            String eventId,
            String eventType,
            Long bookingId,
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
                status,
                timestamp
        );

        repository.saveAndFlush(event);
    }

    // ============================================
    // 2. CONSULTAR TODOS LOS EVENTOS
    // ============================================

    @Transactional(readOnly = true)
    public List<AuditEvent> getAllEvents() {
        return repository.findAllByOrderByReceivedAtDesc();
    }

    // ============================================
    // 3. CONSULTAR EVENTOS POR RESERVA
    // ============================================

    @Transactional(readOnly = true)
    public List<AuditEvent> getEventsByBookingId(Long bookingId) {
        return repository.findByBookingIdOrderByReceivedAtDesc(
                bookingId
        );
    }
}
