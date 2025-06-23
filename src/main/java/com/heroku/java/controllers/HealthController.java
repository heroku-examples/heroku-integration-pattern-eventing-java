package com.heroku.java.controllers;

import com.heroku.java.services.PricingEngineService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Minimal health and monitoring endpoints for Heroku deployment
 * The core business logic runs via Pub/Sub subscription, not HTTP endpoints
 */
@RestController
public class HealthController {

    @Autowired
    private PricingEngineService pricingEngineService;

    @Value("${salesforce.pubsub.enabled:true}")
    private boolean pubSubEnabled;

    @Value("${salesforce.pubsub.topic:/data/OpportunityChangeEvent}")
    private String pubSubTopic;

    /**
     * Health check endpoint for Heroku and monitoring
     */
    @GetMapping("/")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "running");
        status.put("application", "Salesforce Pub/Sub CDC Processor");
        status.put("pubsub_enabled", pubSubEnabled);
        status.put("pubsub_topic", pubSubTopic);
        status.put("description", "Processes Salesforce Change Data Capture events via Pub/Sub API");
        return ResponseEntity.ok(status);
    }

    /**
     * Simple endpoint to trigger sample data creation (for testing)
     */
    @PostMapping("/admin/create-sample-data")
    public ResponseEntity<Map<String, String>> createSampleData(
            @RequestParam(defaultValue = "100") Integer numberOfOpportunities) {
        String jobId = pricingEngineService.createSampleData(numberOfOpportunities);
        Map<String, String> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("message", "Sample data creation job queued");
        return ResponseEntity.ok(response);
    }

    /**
     * Simple endpoint to trigger sample data cleanup (for testing)
     */
    @PostMapping("/admin/delete-sample-data")
    public ResponseEntity<Map<String, String>> deleteSampleData() {
        String jobId = pricingEngineService.deleteSampleData();
        Map<String, String> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("message", "Sample data deletion job queued");
        return ResponseEntity.ok(response);
    }

    /**
     * Display application information
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> info() {
        Map<String, Object> info = new HashMap<>();
        info.put("name", "Salesforce Pub/Sub CDC Sample");
        info.put("description", "Demonstrates processing Salesforce Change Data Capture events via Pub/Sub API");
        info.put("architecture", "Salesforce CDC → Pub/Sub gRPC → Avro Processing → Job Queue");
        info.put("authentication", "Heroku AppLink");
        
        Map<String, String> endpoints = new HashMap<>();
        endpoints.put("GET /", "Health check");
        endpoints.put("GET /info", "Application information");
        endpoints.put("POST /admin/create-sample-data", "Create sample Opportunities (testing)");
        endpoints.put("POST /admin/delete-sample-data", "Delete sample data (testing)");
        info.put("endpoints", endpoints);
        
        return ResponseEntity.ok(info);
    }
} 
