package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    private final String topic;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaProducer(
            @Value("${general.kafka-topic}") String topic,
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
    }

    public void send(String transactionLine) {
        try {
            String[] transactionData = transactionLine.split(", ");

            Transaction transaction = new Transaction(
                    Long.parseLong(transactionData[0]),
                    Long.parseLong(transactionData[1]),
                    Float.parseFloat(transactionData[2])
            );

            // convert Transaction → JSON String
            String message = objectMapper.writeValueAsString(transaction);

            kafkaTemplate.send(topic, message);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}