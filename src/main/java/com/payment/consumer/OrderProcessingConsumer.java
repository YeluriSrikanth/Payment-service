package com.payment.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.dto.Order;
import com.payment.dto.User;
import com.payment.entity.Payment;
import com.payment.repository.PaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Date;
import java.util.function.Consumer;

@Component
public class OrderProcessingConsumer {

    public static final String USER_SERVICE_URL = "http://localhost:9090/users";
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private PaymentRepository repository;

    @RetryableTopic(attempts ="3", backoff=@Backoff(delay = 3000,multiplier = 1.5,maxDelay = 15000),exclude = {NullPointerException.class})
    @KafkaListener(topics = "ORDER_PAYMENT_TOPIC")
    public void processOrder(String orderJsonString, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,@Header(KafkaHeaders.OFFSET) long offset) {
        System.out.println("Kafka listener////////////////////////////");

            try {
                Order order = new ObjectMapper()
                        .readValue(orderJsonString, Order.class);
                System.out.println("Order object..."+order);
                //build payment request
                Payment payment = Payment.builder()
                        .payMode(order.getPaymentMode())
                        .amount(order.getPrice())
                        .paidDate(new Date())
                        .userId(order.getUserId())
                        .orderId(order.getOrderId())
                        .build();
                System.out.println("Payment .."+payment.getPayMode());
                if (payment.getPayMode().equals("COD")) {
                    payment.setPaymentStatus("PENDING");
                    System.out.println("if block ---------------");

                    //do rest call to user service (validate available amount) -> not required
                } else {
                    //validation
System.out.println("else block..........");
                    User user = restTemplate.getForObject(USER_SERVICE_URL + "/" + payment.getUserId(), User.class);
                    System.out.println("User Details-----"+user);
                    if (payment.getAmount() > user.getAvailableAmount()) {
                        throw new RuntimeException("Insufficient amount !");
                    } else {
                        payment.setPaymentStatus("PAID");
                        System.out.println(payment);
                        restTemplate.put(USER_SERVICE_URL + "/" + payment.getUserId() + "/" + payment.getAmount(), null);
                    }

                }
                repository.save(payment);
            } catch (JsonProcessingException e) {
                e.printStackTrace();//log
            }

    }

    @DltHandler
    public void listenDlt(String orderJsonString, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,@Header(KafkaHeaders.OFFSET) long offset){
        System.out.println(topic+"-------------"+offset);

    }




}