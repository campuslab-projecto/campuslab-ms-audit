
package cl.duoc.campuslab.audit.repository;

import cl.duoc.campuslab.audit.entity.AuditEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface AuditEventRepository
        extends JpaRepository<AuditEvent, Long> {

    // Buscar un evento por su identificador único.
    Optional<AuditEvent> findByEventId(String eventId);

    // Comprobar si un evento ya fue registrado.
    boolean existsByEventId(String eventId);

    // Consultar el historial del más reciente al más antiguo.
    List<AuditEvent> findAllByOrderByReceivedAtDesc();

    // Consultar eventos asociados a una reserva.
    List<AuditEvent> findByBookingIdOrderByReceivedAtDesc(
            Long bookingId
    );
}
