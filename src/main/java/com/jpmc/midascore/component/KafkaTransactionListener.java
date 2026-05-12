package com.jpmc.midascore.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public KafkaTransactionListener(
            UserRepository userRepository,
            TransactionRepository transactionRepository
    ) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(String message) {

        try {

            ObjectMapper mapper = new ObjectMapper();

            Transaction transaction =
                    mapper.readValue(message, Transaction.class);

            UserRecord sender =
                    userRepository.findById(transaction.getSenderId());

            UserRecord recipient =
                    userRepository.findById(transaction.getRecipientId());

            if (sender == null || recipient == null) {
                return;
            }

            if (sender.getBalance() < transaction.getAmount()) {
                return;
            }

            RestTemplate restTemplate = new RestTemplate();

            Incentive incentive =
                    restTemplate.postForObject(
                            "http://localhost:8080/incentive",
                            transaction,
                            Incentive.class
                    );

            float incentiveAmount = 0;

            if (incentive != null) {
                incentiveAmount = incentive.getAmount();
            }

            sender.setBalance(
                    sender.getBalance() - transaction.getAmount()
            );

            recipient.setBalance(
                    recipient.getBalance()
                            + transaction.getAmount()
                            + incentiveAmount
            );

            userRepository.save(sender);
            userRepository.save(recipient);

            TransactionRecord transactionRecord =
                    new TransactionRecord(
                            sender,
                            recipient,
                            transaction.getAmount(),
                            incentiveAmount
                    );

            transactionRepository.save(transactionRecord);

            UserRecord wilbur = userRepository.findById(5);

            if (wilbur != null) {
                System.out.println(
                        "WILBUR BALANCE = "
                                + wilbur.getBalance()
                );
            }

            System.out.println("Processed: " + transaction);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}