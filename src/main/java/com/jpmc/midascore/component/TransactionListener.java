package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;

import jakarta.transaction.Transactional;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public TransactionListener(DatabaseConduit databaseConduit, RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplate;
    }
    
    @Transactional
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "transaction-listener", properties = {"auto.offset.reset=earliest"})
    public void listen(Transaction transaction) {

        // Get the recpient, sender and amount sent for the transaction
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        float amount = transaction.getAmount();

        // If either then recpient or sender doesn't exist then discard the transaction
        if (recipient == null || sender == null) return;

        float recipientBalance = recipient.getBalance();
        float senderBalance = sender.getBalance();

        // If the sender doesn't have enough money then discard the transaction
        if (senderBalance < amount) return;

        // Get the incentive
        Incentive response = restTemplate.postForObject("http://localhost:8080/incentive", sender, Incentive.class);
        float incentive = (response != null) ? response.getAmount() : 0.0f;

        // Update the recpient's and sender's balances
        recipient.setBalance(recipientBalance + amount + incentive);
        sender.setBalance(senderBalance - amount);

        // Save the updated balances
        databaseConduit.save(recipient);
        databaseConduit.save(sender);

        // Record and save the transaction
        TransactionRecord record = new TransactionRecord(sender, recipient, amount, incentive);
        databaseConduit.save(record);
    }
}
