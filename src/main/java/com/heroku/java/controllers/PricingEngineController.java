package com.heroku.java.controllers;

import com.heroku.java.services.PricingEngineService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for the Pricing Engine application
 * Provides endpoints for health checks, sample data management, and application info
 */
@RestController
public class PricingEngineController {

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
    @PostMapping("/api/datacreate")
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
    @PostMapping("/api/datadelete")
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
        endpoints.put("POST /api/datacreate", "Create sample Opportunities (testing)");
        endpoints.put("POST /api/datadelete", "Delete sample data (testing)");
        endpoints.put("POST /api/testevent", "Simulate CDC event for testing");
        info.put("endpoints", endpoints);
        
        return ResponseEntity.ok(info);
    }

    /**
     * Test endpoint to simulate CDC events locally (for testing)
     */
    @PostMapping("/api/testevent")
    public ResponseEntity<Map<String, String>> testCdcEvent(@RequestBody Map<String, Object> cdcEvent) {
        try {
            // Convert the raw CDC event to our ChangeDataCaptureEvent model
            com.heroku.java.model.ChangeDataCaptureEvent changeEvent = convertToChangeEvent(cdcEvent);
            
            // Process the event directly through the PricingEngineService
            pricingEngineService.generateQuote(changeEvent);
            
            Map<String, String> response = new HashMap<>();
            response.put("message", "CDC event processed successfully");
            response.put("recordIds", String.join(",", changeEvent.ChangeEventHeader.recordIds));
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, String> response = new HashMap<>();
            response.put("error", "Failed to process CDC event: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Convert raw CDC event map to ChangeDataCaptureEvent model
     */
    @SuppressWarnings("unchecked")
    private com.heroku.java.model.ChangeDataCaptureEvent convertToChangeEvent(Map<String, Object> cdcEvent) {
        com.heroku.java.model.ChangeDataCaptureEvent changeEvent = new com.heroku.java.model.ChangeDataCaptureEvent();
        
        // Extract ChangeEventHeader
        Map<String, Object> headerMap = (Map<String, Object>) cdcEvent.get("ChangeEventHeader");
        if (headerMap != null) {
            changeEvent.ChangeEventHeader = new com.heroku.java.model.ChangeDataCaptureEvent.ChangeEventHeader();
            changeEvent.ChangeEventHeader.entityName = (String) headerMap.get("entityName");
            changeEvent.ChangeEventHeader.changeType = (String) headerMap.get("changeType");
            changeEvent.ChangeEventHeader.transactionKey = (String) headerMap.get("transactionKey");
            
            // Convert recordIds from List to String array
            Object recordIdsObj = headerMap.get("recordIds");
            if (recordIdsObj instanceof java.util.List) {
                java.util.List<String> recordIdsList = (java.util.List<String>) recordIdsObj;
                changeEvent.ChangeEventHeader.recordIds = recordIdsList.toArray(new String[0]);
            }
        }
        
        return changeEvent;
    }
} 
