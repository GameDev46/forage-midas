package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;

import jakarta.transaction.Transactional;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private final DatabaseConduit databaseConduit;

    public TransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
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

        // Update the recpient's and sender's balances
        recipient.setBalance(recipientBalance + amount);
        sender.setBalance(senderBalance - amount);

        // Save the updated balances
        databaseConduit.save(recipient);
        databaseConduit.save(sender);

        // Record and save the transaction
        TransactionRecord record = new TransactionRecord(sender, recipient, amount);
        databaseConduit.save(record);
    }
}
