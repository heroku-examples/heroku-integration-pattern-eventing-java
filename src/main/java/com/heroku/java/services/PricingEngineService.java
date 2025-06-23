package com.heroku.java.services;

import com.heroku.java.config.SalesforceClient;
import com.heroku.java.model.ChangeDataCaptureEvent;
import com.sforce.soap.partner.PartnerConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Service that processes Salesforce CDC events and manages job queuing for quote generation and sample data
 */
@Service
public class PricingEngineService {

    private static final Logger logger = LoggerFactory.getLogger(PricingEngineService.class);

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private SalesforceClient salesforceClient;

    /**
     * Be sure to use a shared state for buffering when scaling the web process horizontally
     */
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<String>> transactionCache = new ConcurrentHashMap<>();
    private volatile String lastTransactionKey = null;

    /**
     * Generate quotes for opportunities based on CDC events
     * Called when opportunities are updated and need quote generation
     * @param changeEvent The CDC event containing Opportunity change data
     */
    public void generateQuote(ChangeDataCaptureEvent changeEvent) {
        logger.info("Processing CDC event for entity: {}, changeType: {}", 
                   changeEvent.ChangeEventHeader.entityName, 
                   changeEvent.ChangeEventHeader.changeType);
        
        if (changeEvent == null || changeEvent.ChangeEventHeader == null) {
            logger.error("Invalid CDC event format - missing header");
            return;
        }
        
        // Extract transactionKey and buffer record changes to allow for optimal job processing (flush buffer every 15 seconds)
        String transactionKey = changeEvent.ChangeEventHeader.transactionKey;
        String[] recordIds = changeEvent.ChangeEventHeader.recordIds;
        
        if (transactionKey == null || recordIds == null) {
            logger.warn("TransactionKey or recordIds are missing in the event.");
            return;
        }
        
        processTransactionKey(transactionKey, recordIds);
        logger.info("Processed CDC event with transaction key: {}", transactionKey);
    }

    /**
     * Starts a job to create a large number of Opportunity records.
     */
    public String createSampleData(Integer numberOfOpportunities) {
        logger.info("Creating {} sample Opportunities", numberOfOpportunities);
        return enqueueJob("dataQueue", "create:" + numberOfOpportunities);
    }

    /**
     * Starts a job to delete generated Quotes.
     */
    public String deleteSampleData() {
        logger.info("Deleting sample Quote data");
        return enqueueJob("dataQueue", "delete");
    }

    /**
     * Either buffer the recordIds if the transactionKey is the same as the last one or flush if its different
     * @param transactionKey
     * @param recordIds
     * @return Job ID or "Buffering"
     */
    private synchronized void processTransactionKey(String transactionKey, String[] recordIds) {
        if (!transactionKey.equals(lastTransactionKey)) {
            if (lastTransactionKey != null && transactionCache.containsKey(lastTransactionKey)) {
                enqueueAndClear(lastTransactionKey);
            }
            lastTransactionKey = transactionKey;
            transactionCache.put(transactionKey, new CopyOnWriteArrayList<>());
        }
        CopyOnWriteArrayList<String> records = transactionCache.get(transactionKey);
        if (records != null) {
            records.addAll(Arrays.asList(recordIds));
        }
    }

    /**
     * Enque a job to process the recordIds received and clear the buffer
     * @param transactionKey
     */
    private void enqueueAndClear(String transactionKey) {
        List<String> recordIds = transactionCache.remove(transactionKey);
        String messageData = String.join(",", recordIds);
        logger.info("Enqueuing job for transactionKey: {} with records: {}", transactionKey, messageData);
        String jobId = enqueueJob("quoteQueue", messageData);
        logger.info("Job {} enqueued for transactionKey: {}", jobId, transactionKey);
    }

    /**
     * Explicitly clears the buffer every 15 seconds
     */
    @Scheduled(fixedRate = 15000)
    public void flushTransactionQueue() {
        if (lastTransactionKey != null) {
            enqueueAndClear(lastTransactionKey);
            lastTransactionKey = null;
        }
    }

    /**
     * Enqueue the job by posting a message to the given channel along with Salesforce connection details
     * @param channel
     * @param message
     * @param httpServletRequest
     * @return
     */
    private String enqueueJob(String channel, String message) {
        // Generate a unique Job ID for this request
        String jobId = UUID.randomUUID().toString();
        // Get Salesforce session from Heroku Integration assume first connectoin declared
        PartnerConnection connection = salesforceClient.getConnections().values().iterator().next();
        if (connection == null) {
            logger.error("Salesforce connection is not available.");
            throw new RuntimeException("Salesforce connection is not available.");
        }
        try {
            // Store session info in Redis for the worker to use
            String sessionId = connection.getSessionHeader().getSessionId();
            String instanceUrl = connection.getConfig().getServiceEndpoint(); // Extract instance URL
            redis.opsForValue().set("salesforce:session:" + jobId, sessionId);
            redis.opsForValue().set("salesforce:instance:" + jobId, instanceUrl);
            // Enqueue job
            redis.convertAndSend(channel, jobId + ":" + message);
            logger.info("Job enqueued with ID: {} for message: {} to channel: {}", jobId, message, channel);

        } catch (Exception e) {
            logger.error("Error interacting with Redis: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process the request due to an internal error.", e);
        }
        return jobId;
    }

}
