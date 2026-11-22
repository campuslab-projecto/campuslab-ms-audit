
package cl.duoc.campuslab.audit.dto;

import java.time.LocalDateTime;

public record BookingKafkaEvent(
        String eventId,
        String eventType,
        Long bookingId,
        String resourceId,
        String status,
        LocalDateTime timestamp
) {
}
