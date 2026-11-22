package cl.duoc.campuslab.audit;

import cl.duoc.campuslab.audit.entity.AuditEvent;
import cl.duoc.campuslab.audit.repository.AuditEventRepository;
import cl.duoc.campuslab.audit.service.AuditEventService;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:audit-ep6-test;DB_CLOSE_DELAY=-1",
        "spring.jpa.hibernate.ddl-auto=create-drop"
})
@Transactional
class AuditEventServiceIntegrationTest {

    @Autowired
    private AuditEventService service;

    @Autowired
    private AuditEventRepository repository;

    @Test
    void debeGuardarEventoConResourceId() {

        String eventId = "audit-ep6-resource-001";

        service.saveEvent(
                eventId,
                "BOOKING_CREATED",
                1001L,
                "LAB-EP6-01",
                "SOLICITADA",
                LocalDateTime.now()
        );

        AuditEvent evento = repository.findByEventId(eventId)
                .orElseThrow();

        assertEquals(1001L, evento.getBookingId());
        assertEquals("LAB-EP6-01", evento.getResourceId());
        assertEquals("BOOKING_CREATED", evento.getEventType());
    }

    @Test
    void debeAceptarEventoAntiguoSinResourceId() {

        String eventId = "audit-ep6-legacy-001";

        service.saveEvent(
                eventId,
                "BOOKING_CREATED",
                1002L,
                "SOLICITADA",
                LocalDateTime.now()
        );

        AuditEvent evento = repository.findByEventId(eventId)
                .orElseThrow();

        assertNull(evento.getResourceId());
        assertEquals(1002L, evento.getBookingId());
    }

    @Test
    void noDebeGuardarDosVecesElMismoEvento() {

        String eventId = "audit-ep6-duplicate-001";

        LocalDateTime fecha = LocalDateTime.now();

        service.saveEvent(
                eventId,
                "BOOKING_CREATED",
                1003L,
                "LAB-EP6-02",
                "SOLICITADA",
                fecha
        );

        service.saveEvent(
                eventId,
                "BOOKING_CREATED",
                1003L,
                "LAB-EP6-02",
                "SOLICITADA",
                fecha
        );

        List<AuditEvent> eventos =
                repository.findAll().stream()
                        .filter(e -> eventId.equals(e.getEventId()))
                        .toList();

        assertEquals(1, eventos.size());
    }
}
