package com.heroku.java.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.heroku.java.config.SalesforceClient;
import com.sforce.soap.partner.PartnerConnection;

import io.cloudevents.CloudEvent;
import reactor.core.publisher.Mono;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Exposed REST endpoints to Salesforce that will enqueue jobs to generate Quotes and manage sample data
 */
@RestController
@RequestMapping("/api/")
public class PricingEngineService {

    private static final Logger logger = LoggerFactory.getLogger(PricingEngineService.class);

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private SalesforceClient salesforceClient;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Be sure to use a shared state for buffering when scaling the web process horizontally
     */
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<String>> transactionCache = new ConcurrentHashMap<>();
    private volatile String lastTransactionKey = null;

    /**
     * Calculate pricing and generate quotes from Opportunities contained in the Salesforce CDC event data
     * @param request
     * @param httpServletRequest
     * @return
     */
    @PostMapping("/generatequotes")
    public void generatequotes(@RequestBody CloudEvent cloudEvent) {
        logger.info("Received CloudEvent: ID: {}, Source: {}, Type: {}", cloudEvent.getId(), cloudEvent.getSource(), cloudEvent.getType());
        // Extract recordIds from event data per Saleforce CDC event format
        ChangeDataCaptureEvent changeEvent = extractChangeEvent(cloudEvent);
        if (changeEvent == null || changeEvent.ChangeEventHeader == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid CDC event format.");
        }
        // Extract transactionKey and buffer record changes to allow for optimal job processing (flush buffer every 15 seconds)
        String transactionKey = changeEvent.ChangeEventHeader.transactionKey;
        String[] recordIds = changeEvent.ChangeEventHeader.recordIds;
        if (transactionKey == null || recordIds == null) {
            logger.warn("TransactionKey or recordIds are missing in the event.");
        }
        processTransactionKey(transactionKey, recordIds);
        logger.info("Handled CloudEvent: ID: {}", cloudEvent.getId());
    }

    /**
     * Starts a job to create a large number of Opportunity records.
     */
    @PostMapping("/data/create")
    public Mono<DataJobResponse> datacreate(@RequestParam(defaultValue = "100") Integer numberOfOpportunities) {
        logger.info("Received Opportunity data creation request to create {} Opportunities", numberOfOpportunities);
        // Submit the job to the queue
        String jobId = enqueueJob("dataQueue", "create:" + numberOfOpportunities);
        DataJobResponse response = new DataJobResponse();
        response.jobId = jobId;
        return Mono.just(response);
    }

    /**
     * Starts a job to delete generated Quotes.
     */
    @PostMapping("/data/delete")
    public Mono<DataJobResponse> datadelete() {
        logger.info("Received Quote data deletion request");
        // Submit the job to the queue
        String jobId = enqueueJob("dataQueue", "delete");
        DataJobResponse response = new DataJobResponse();
        response.jobId = jobId;
        return Mono.just(response);
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
     * Parse the CloudEvent to extract the Salesforce ChangeDataCaptureEvent and header informaiton about the records that have changed
     * @param cloudEvent
     * @return
     */
    private ChangeDataCaptureEvent extractChangeEvent(CloudEvent cloudEvent) {
        try {
            byte[] data = cloudEvent.getData().toBytes();
            if (data != null) {
                return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), ChangeDataCaptureEvent.class);
            }
        } catch (Exception e) {
            logger.error("Error processing CloudEvent data: {}", e.getMessage(), e);
        }
        return null;
    }

    /**
     * Response includes the unique job ID processing the request.
     */
    public static class DataJobResponse {
        // Unique job ID for tracking the worker process
        public String jobId;
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
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Salesforce connection is not available.");
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
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to process the request due to an internal error.");
        }
        return jobId;
    }

    /**
     * General purpose class represneting ChangeDataCaptureEvent passed in the 'data' portion of the CloudEvent
     * https://developer.salesforce.com/docs/atlas.en-us.254.0.change_data_capture.meta/change_data_capture/cdc_message_structure.htm
     */
    private static class ChangeDataCaptureEvent {
        public ChangeEventHeader ChangeEventHeader;
    }        
    /**
     * General purpose class represneting ChangeEventHeader data
     * https://developer.salesforce.com/docs/atlas.en-us.254.0.change_data_capture.meta/change_data_capture/cdc_message_structure.htm
     */
    @SuppressWarnings("unused")        
    private static class ChangeEventHeader {
        public String entityName;
        public String[] recordIds;
        public String changeType;
        public String changeOrigin;
        public String transactionKey;
        public Integer sequenceNumber;
        public Long commitTimestamp;
        public Long commitNumber;
        public String commitUser;
        public String[] nulledFields;
        public String[] diffFields;
        public String[] changedFields;        
    }
}
