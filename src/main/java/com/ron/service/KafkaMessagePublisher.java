package com.ron.service;

import com.ron.dto.Customer;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

//https://github.com/Java-Techie-jt/kafka-producer-example
@Service
@RequiredArgsConstructor
public class KafkaMessagePublisher {

    private final KafkaTemplate<String, Object> template;

    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> future = template.send("javatechie-demo2", message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" + message + "] due to:[" + ex.getMessage() + "]");
            }
        });
    }


    public void sendEventsToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> future = template.send("javatechie-demo", customer);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent message=[" + customer.toString() + "] with offset=[" + result.getRecordMetadata()
                            .offset() + "]");
                } else {
                    System.out.println("Unable to send message=[" + customer.toString() + "] due to:[" + ex.getMessage() + "]");
                }
            });
        } catch (Exception e) {
            System.out.println("Error : " + e.getMessage());
        }

    }
}
