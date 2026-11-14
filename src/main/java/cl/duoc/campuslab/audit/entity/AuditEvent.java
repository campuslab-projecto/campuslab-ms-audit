
package cl.duoc.campuslab.audit.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(
        name = "audit_events",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_audit_event_id",
                columnNames = "event_id"
        )
)
public class AuditEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(nullable = false)
    private String eventType;

    private Long bookingId;

    private String status;

    private LocalDateTime timestamp;

    @Column(nullable = false)
    private LocalDateTime receivedAt;

    public AuditEvent() {
    }

    public AuditEvent(
            String eventId,
            String eventType,
            Long bookingId,
            String status,
            LocalDateTime timestamp
    ) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.bookingId = bookingId;
        this.status = status;
        this.timestamp = timestamp;
        this.receivedAt = LocalDateTime.now();
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public Long getBookingId() {
        return bookingId;
    }

    public String getStatus() {
        return status;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }
}
