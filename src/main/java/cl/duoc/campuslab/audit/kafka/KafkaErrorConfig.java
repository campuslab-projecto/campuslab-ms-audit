
package cl.duoc.campuslab.audit.kafka;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfig {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaErrorConfig.class);

    @Bean
    public DefaultErrorHandler auditKafkaErrorHandler(
            KafkaTemplate<Object, Object> kafkaTemplate
    ) {

        // Enviar eventos fallidos a la misma particion de la DLT.
        DeadLetterPublishingRecoverer recuperador =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (registro, excepcion) -> {

                            String destino =
                                    registro.topic() + ".DLT";

                            log.error(
                                    "AUDIT KAFKA: Evento fallido. " +
                                            "Topico={}, particion={}, offset={}, " +
                                            "destino={}, error={}",
                                    registro.topic(),
                                    registro.partition(),
                                    registro.offset(),
                                    destino,
                                    excepcion.toString()
                            );

                            return new TopicPartition(
                                    destino,
                                    registro.partition()
                            );
                        }
                );

        // Si falla la publicacion a DLT, no dar por recuperado.
        recuperador.setFailIfSendResultIsError(true);

        // Dos reintentos, separados por un segundo.
        FixedBackOff reintentos =
                new FixedBackOff(1000L, 2L);

        DefaultErrorHandler manejador =
                new DefaultErrorHandler(
                        recuperador,
                        reintentos
                );

        // Confirmar el offset una vez recuperado en la DLT.
        manejador.setCommitRecovered(true);

        return manejador;
    }
}
