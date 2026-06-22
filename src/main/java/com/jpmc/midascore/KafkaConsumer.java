package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group",
            properties = {
                    "spring.json.value.default.type=com.jpmc.midascore.foundation.Transaction"
            }
    )
    public void listen(Transaction transaction) {
        System.out.println("Transaction received: " + transaction);
        System.out.println("Amount = " + transaction.getAmount());
    }
}