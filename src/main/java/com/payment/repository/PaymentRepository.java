package com.payment.repository;

import com.payment.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment,Integer> {
   // Payment findByOrderId(String orderId);

}