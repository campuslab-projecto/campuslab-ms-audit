
package cl.duoc.campuslab.audit.controller;
import cl.duoc.campuslab.audit.entity.AuditEvent;
import cl.duoc.campuslab.audit.service.AuditEventService;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/audit")
public class AuditController {

    private final AuditEventService auditEventService;

    public AuditController(AuditEventService auditEventService) {
        this.auditEventService = auditEventService;
    }

    // Consultar todos los eventos de auditoría.
    @GetMapping
    public ResponseEntity<List<AuditEvent>> getAllEvents() {

        return ResponseEntity.ok(
                auditEventService.getAllEvents()
        );
    }

    // Consultar eventos de una reserva específica.
    @GetMapping("/bookings/{bookingId}")
    public ResponseEntity<List<AuditEvent>> getByBookingId(
            @PathVariable Long bookingId
    ) {

        return ResponseEntity.ok(
                auditEventService.getEventsByBookingId(bookingId)
        );
    }
}
