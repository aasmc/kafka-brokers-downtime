package com.example.kafkadowntime;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ExperimentProducer {

    private static final Logger log = LoggerFactory.getLogger(ExperimentProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;
    private final AtomicLong sequence = new AtomicLong();

    public ExperimentProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${experiment.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Scheduled(fixedRateString = "${experiment.producer.interval-ms}")
    public void publish() {
        long id = sequence.incrementAndGet();
        String key = Long.toString(id);
        String value = "sequence=" + id + ",timestamp=" + Instant.now();
        try {
            kafkaTemplate.send(topic, key, value).whenComplete((result, error) -> {
                if (error == null) {
                    log.info("PRODUCER_ACK sequence={} partition={} offset={}",
                            id,
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    log.error("PRODUCER_ERROR sequence={} error={}: {}",
                            id, error.getClass().getName(), error.getMessage(), error);
                }
            });
        } catch (Exception error) {
            log.error("PRODUCER_SEND_ERROR sequence={} error={}: {}",
                    id, error.getClass().getName(), error.getMessage(), error);
        }
    }
}
