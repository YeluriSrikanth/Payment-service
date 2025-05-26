package com.payment.service;

import com.payment.advice.UserNotFoundException;
import com.payment.entity.Payment;
import com.payment.repository.PaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {
    @Autowired
    private PaymentRepository repository;

    public ResponseEntity<Payment> getByOrderId(String orderId) throws UserNotFoundException {
        return ResponseEntity.ok(repository.findById(Integer.valueOf(orderId))
                .orElseThrow(() -> new UserNotFoundException("user not found", "aaaaaaaaaa", "vvvvvvvvvvvv")));
    }
}