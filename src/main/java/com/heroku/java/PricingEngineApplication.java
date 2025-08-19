package com.heroku.java;

import java.util.concurrent.CountDownLatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PricingEngineApplication {

    public static void main(String[] args) throws InterruptedException {
        ApplicationContext context = SpringApplication.run(PricingEngineApplication.class, args);
        
        // For worker processes, keep the application running to process jobs
        if(context.getEnvironment().matchesProfiles("worker")) {
            System.out.println("Worker profile is active. Processing Redis job queue...");
            CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        } else {
            System.out.println("Web server started. Pub/Sub subscriber listening for Salesforce events...");
        }
    }
}
