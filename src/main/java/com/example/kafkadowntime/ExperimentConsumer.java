package com.example.kafkadowntime;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ExperimentConsumer {

    private static final Logger log = LoggerFactory.getLogger(ExperimentConsumer.class);

    @KafkaListener(topics = "${experiment.topic}")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("CONSUMER_RECORD key={} value={} partition={} offset={}",
                record.key(), record.value(), record.partition(), record.offset());
    }
}
