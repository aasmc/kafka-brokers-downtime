package com.example.kafkadowntime;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaDowntimeApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDowntimeApplication.class, args);
    }
}
