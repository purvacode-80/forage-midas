package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public KafkaConsumer(DatabaseConduit databaseConduit,
                         RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group",
            properties = {
                    "spring.json.value.default.type=com.jpmc.midascore.foundation.Transaction"
            }
    )
    public void listen(Transaction transaction) {

        UserRecord sender =
                databaseConduit.findById(transaction.getSenderId());

        UserRecord recipient =
                databaseConduit.findById(transaction.getRecipientId());

        // Validate sender and recipient
        if (sender == null || recipient == null) {
            return;
        }

        // Validate balance
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        // Call Incentive API
        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
        );

        float incentiveAmount = 0.0f;

        if (incentive != null) {
            incentiveAmount = incentive.getAmount();
        }

        // Update balances
        sender.setBalance(
                sender.getBalance() - transaction.getAmount()
        );

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentiveAmount
        );

        // Save updated users
        databaseConduit.save(sender);
        databaseConduit.save(recipient);
    }
}