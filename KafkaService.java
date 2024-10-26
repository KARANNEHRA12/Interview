package com.programmingtechie.orderService.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.programmingtechie.orderService.event.OrderPlacedEvent;

public class KafkaService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void publishToKafka(String topic, String key, String value) {

        ListenableFuture<SendResult<String, String>> future = (ListenableFuture<SendResult<String, String>>) kafkaTemplate
                .send(topic, key, value);
        Futures.addCallback(future, getListenableFutureCallback(topic));

    }

    public void publishToKafka(String topic, Integer partition, String key, String value) {

        ListenableFuture<SendResult<String, String>> future = (ListenableFuture<SendResult<String, String>>) kafkaTemplate
                .send(topic, partition, key, value);
        Futures.addCallback(future, getListenableFutureCallback(topic));

    }

    private FutureCallback<SendResult<String, String>> getListenableFutureCallback(final String topic) {
        return new FutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Message sent successfully: " + result.getRecordMetadata());
            }

            @Override
            public void onFailure(Throwable t) {
                System.err.println("Message sending failed: " + t.getMessage());
            }
        };
    }

}
