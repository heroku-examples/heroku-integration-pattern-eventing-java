package com.heroku.java.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.heroku.java.config.SalesforceClient;
import com.heroku.java.model.ChangeDataCaptureEvent;
import com.heroku.java.model.ChangeDataCaptureEvent.ChangeEventHeader;
import com.sforce.soap.partner.PartnerConnection;

// Generated Salesforce Pub/Sub API classes
import com.salesforce.eventbus.protobuf.ConsumerEvent;
import com.salesforce.eventbus.protobuf.FetchRequest;
import com.salesforce.eventbus.protobuf.FetchResponse;
import com.salesforce.eventbus.protobuf.PubSubGrpc;
import com.salesforce.eventbus.protobuf.ReplayPreset;
import com.salesforce.eventbus.protobuf.SchemaInfo;
import com.salesforce.eventbus.protobuf.SchemaRequest;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;

/**
 * Service that subscribes to Salesforce Change Data Capture events via Pub/Sub API
 * using gRPC and processes them through batching for optimal performance
 */
@Service
public class SalesforcePubSubSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(SalesforcePubSubSubscriber.class);
    
    @Value("${salesforce.pubsub.host:api.pubsub.salesforce.com}")
    private String pubSubHost;
    
    @Value("${salesforce.pubsub.port:7443}")
    private int pubSubPort;
    
    @Value("${salesforce.pubsub.topic:/data/OpportunityChangeEvent}")
    private String pubSubTopic;
    
    // Batching configuration
    @Value("${salesforce.pubsub.batch.size:50}")
    private int batchSize;
    
    @Value("${salesforce.pubsub.batch.timeout:5000}")
    private long batchTimeoutMs;
    
    @Value("${salesforce.pubsub.fetch.size:100}")
    private int fetchSize;
    
    @Autowired
    private SalesforceClient salesforceClient;
    
    @Autowired
    private PricingEngineService pricingEngineService;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    private ManagedChannel channel;
    private volatile boolean isSubscribed = false;
    private volatile boolean batchProcessorRunning = true;
    
    // Batching infrastructure
    private final BlockingQueue<ChangeDataCaptureEvent> eventQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService batchProcessor = Executors.newScheduledThreadPool(2);
    private final Map<String, List<ChangeDataCaptureEvent>> transactionBatches = new ConcurrentHashMap<>();
    private volatile ScheduledFuture<?> batchTimeoutTask;
    
    /**
     * Start subscription when application is ready
     */
    @EventListener(ApplicationReadyEvent.class)
    public void startSubscription() {
        if (shouldStartSubscription()) {
            logger.info("Starting Salesforce Pub/Sub subscription with batching...");
            logger.info("Batch configuration - Size: {}, Timeout: {}ms, Fetch: {}", 
                       batchSize, batchTimeoutMs, fetchSize);
            
            // Start batch processing
            startBatchProcessor();
            
            // Start Pub/Sub subscription
            CompletableFuture.runAsync(this::subscribeToOpportunityChangeEvents);
        }
    }
    
    /**
     * Check if this instance should start subscription (only for non-worker profiles)
     */
    private boolean shouldStartSubscription() {
        // Only start subscription in web/default profile to avoid duplicate subscriptions
        String profiles = System.getProperty("spring.profiles.active", "");
        return !profiles.contains("worker");
    }
    
    /**
     * Start the batch processing system
     */
    private void startBatchProcessor() {
        logger.info("Starting batch processor thread...");
        // Size-based batch processor
        batchProcessor.execute(() -> {
            logger.info("Batch processor thread started and running");
            List<ChangeDataCaptureEvent> batch = new ArrayList<>();
            
            while (batchProcessorRunning) {
                try {
                    // Collect events up to batch size
                    ChangeDataCaptureEvent event = eventQueue.poll(1, TimeUnit.SECONDS);
                    if (event != null) {
                        logger.info("Batch processor received event from queue, batch size now: {}", batch.size() + 1);
                        batch.add(event);
                        
                        // Process batch when it reaches the configured size
                        if (batch.size() >= batchSize) {
                            processBatch(new ArrayList<>(batch));
                            batch.clear();
                            cancelTimeoutTask();
                        } else if (batch.size() == 1) {
                            // Start timeout timer for first event in batch
                            scheduleTimeoutTask(batch);
                        }
                    } else if (!batch.isEmpty()) {
                        // Process remaining events when queue is empty
                        processBatch(new ArrayList<>(batch));
                        batch.clear();
                        cancelTimeoutTask();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error in batch processor: {}", e.getMessage(), e);
                }
            }
            
            // Process any remaining events
            if (!batch.isEmpty()) {
                processBatch(batch);
            }
        });
    }
    
    /**
     * Schedule timeout task to process batch after timeout period
     */
    private synchronized void scheduleTimeoutTask(List<ChangeDataCaptureEvent> batch) {
        cancelTimeoutTask();
        batchTimeoutTask = batchProcessor.schedule(() -> {
            synchronized (this) {
                if (!batch.isEmpty()) {
                    logger.info("Processing batch due to timeout - Size: {}", batch.size());
                    processBatch(new ArrayList<>(batch));
                    batch.clear();
                }
            }
        }, batchTimeoutMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Cancel the current timeout task
     */
    private synchronized void cancelTimeoutTask() {
        if (batchTimeoutTask != null && !batchTimeoutTask.isDone()) {
            batchTimeoutTask.cancel(false);
            batchTimeoutTask = null;
        }
    }
    
    /**
     * Process a batch of CDC events with transaction grouping
     */
    private void processBatch(List<ChangeDataCaptureEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        
        logger.info("Processing batch of {} CDC events", events.size());
        
        // Group events by transaction key for optimal processing
        Map<String, List<ChangeDataCaptureEvent>> transactionGroups = new HashMap<>();
        List<ChangeDataCaptureEvent> orphanEvents = new ArrayList<>();
        
        for (ChangeDataCaptureEvent event : events) {
            String transactionKey = event.ChangeEventHeader != null ? 
                                  event.ChangeEventHeader.transactionKey : null;
            
            if (transactionKey != null) {
                transactionGroups.computeIfAbsent(transactionKey, k -> new ArrayList<>()).add(event);
            } else {
                orphanEvents.add(event);
            }
        }
        
        // Process transaction groups
        for (Map.Entry<String, List<ChangeDataCaptureEvent>> entry : transactionGroups.entrySet()) {
            String transactionKey = entry.getKey();
            List<ChangeDataCaptureEvent> transactionEvents = entry.getValue();
            
            logger.info("Processing transaction group: {} with {} events", 
                       transactionKey, transactionEvents.size());
            
            // Merge events from same transaction for optimal processing
            ChangeDataCaptureEvent mergedEvent = mergeTransactionEvents(transactionEvents);
            if (mergedEvent != null) {
                pricingEngineService.processChangeDataCaptureEvent(mergedEvent);
            }
        }
        
        // Process orphan events individually
        for (ChangeDataCaptureEvent orphanEvent : orphanEvents) {
            logger.info("Processing orphan event without transaction key");
            pricingEngineService.processChangeDataCaptureEvent(orphanEvent);
        }
        
        logger.info("Completed batch processing - {} transaction groups, {} orphan events", 
                   transactionGroups.size(), orphanEvents.size());
    }
    
    /**
     * Merge multiple events from the same transaction into a single optimized event
     */
    private ChangeDataCaptureEvent mergeTransactionEvents(List<ChangeDataCaptureEvent> events) {
        if (events.isEmpty()) {
            return null;
        }
        
        if (events.size() == 1) {
            return events.get(0);
        }
        
        // Use the first event as base and merge record IDs
        ChangeDataCaptureEvent mergedEvent = events.get(0);
        Set<String> allRecordIds = new HashSet<>();
        Set<String> allChangedFields = new HashSet<>();
        
        for (ChangeDataCaptureEvent event : events) {
            if (event.ChangeEventHeader != null) {
                // Collect all record IDs
                if (event.ChangeEventHeader.recordIds != null) {
                    allRecordIds.addAll(Arrays.asList(event.ChangeEventHeader.recordIds));
                }
                
                // Collect all changed fields
                if (event.ChangeEventHeader.changedFields != null) {
                    allChangedFields.addAll(Arrays.asList(event.ChangeEventHeader.changedFields));
                }
            }
        }
        
        // Update merged event with consolidated data
        mergedEvent.ChangeEventHeader.recordIds = allRecordIds.toArray(new String[0]);
        mergedEvent.ChangeEventHeader.changedFields = allChangedFields.toArray(new String[0]);
        
        logger.debug("Merged {} events into single event with {} record IDs", 
                    events.size(), allRecordIds.size());
        
        return mergedEvent;
    }
    
    /**
     * Subscribe to Opportunity Change Data Capture events via Pub/Sub API
     */
    public void subscribeToOpportunityChangeEvents() {
        try {
            // Get Salesforce connection for authentication
            PartnerConnection connection = salesforceClient.getConnections().values().iterator().next();
            if (connection == null) {
                logger.error("No Salesforce connection available for Pub/Sub subscription");
                return;
            }
            
            String accessToken = connection.getSessionHeader().getSessionId();
            String soapEndpoint = connection.getConfig().getServiceEndpoint();
            
            // Extract the base instance URL from the SOAP endpoint
            // SOAP endpoint format: https://instance.my.salesforce.com/services/Soap/u/XX.0
            URI soapUri = URI.create(soapEndpoint);
            String baseInstanceUrl = soapUri.getScheme() + "://" + soapUri.getHost();
            
            // Get tenant ID (org ID) from SalesforceClient
            String tenantId = salesforceClient.getOrgId();
            if (tenantId == null) {
                // Fallback: extract from hostname if SF_ORG_ID not available
                tenantId = extractTenantId(soapUri.getHost());
            }
            
            logger.info("Connecting to Salesforce Pub/Sub API for tenant: {}", tenantId);
            logger.info("Using instance URL: {}", baseInstanceUrl);
            logger.info("Using access token: {}...", accessToken.substring(0, Math.min(10, accessToken.length())));
            
            // Create gRPC channel
            channel = ManagedChannelBuilder.forAddress(pubSubHost, pubSubPort)
                    .useTransportSecurity()
                    .build();
            
            // Set up authentication metadata with correct header names (case-sensitive!)
            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of("accessToken", Metadata.ASCII_STRING_MARSHALLER), accessToken);
            metadata.put(Metadata.Key.of("instanceUrl", Metadata.ASCII_STRING_MARSHALLER), baseInstanceUrl);
            metadata.put(Metadata.Key.of("tenantId", Metadata.ASCII_STRING_MARSHALLER), tenantId);
            
            logger.info("Pub/Sub subscription setup initiated for topic: {}", pubSubTopic);
            isSubscribed = true;
            
            // Start subscription loop with batching
            subscriptionLoop(metadata);
            
        } catch (Exception e) {
            logger.error("Error setting up Pub/Sub subscription: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Main subscription loop using actual Salesforce Pub/Sub API gRPC streaming
     */
    private void subscriptionLoop(Metadata metadata) {
        logger.info("Starting subscription loop for Opportunity CDC events with batching");
        
        // Create Pub/Sub stub with authentication metadata
        PubSubGrpc.PubSubStub pubSubStub = PubSubGrpc.newStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
        
        // Store reference for inner class access
        final StreamObserver<FetchRequest>[] requestObserverRef = new StreamObserver[1];
        
        // Create bidirectional streaming call
        StreamObserver<FetchRequest> requestObserver = pubSubStub.subscribe(new StreamObserver<FetchResponse>() {
            @Override
            public void onNext(FetchResponse response) {
                logger.info("Received {} events from Pub/Sub API", response.getEventsCount());
                
                // Process each event and add to batch queue
                for (ConsumerEvent consumerEvent : response.getEventsList()) {
                    try {
                        // Extract Avro payload from the event
                        byte[] avroData = consumerEvent.getEvent().getPayload().toByteArray();
                        String schemaId = consumerEvent.getEvent().getSchemaId();
                        
                        // Get the schema for this schema ID
                        Schema avroSchema = getSchemaById(schemaId);
                        if (avroSchema != null) {
                            // Convert to CDC event and add to batch queue
                            ChangeDataCaptureEvent cdcEvent = processAvroEvent(avroData, avroSchema);
                            if (cdcEvent != null) {
                                logger.info("Adding CDC event to batch queue, queue size: {}", eventQueue.size() + 1);
                                eventQueue.offer(cdcEvent);
                            }
                        } else {
                            logger.error("Could not retrieve schema for ID: {}", schemaId);
                        }
                        
                    } catch (Exception e) {
                        logger.error("Error processing consumer event: {}", e.getMessage(), e);
                    }
                }
                
                // Request more events with larger fetch size for better batching
                if (isSubscribed && response.getPendingNumRequested() == 0) {
                    FetchRequest nextRequest = FetchRequest.newBuilder()
                            .setNumRequested(fetchSize)
                            .build();
                    if (requestObserverRef[0] != null) {
                        requestObserverRef[0].onNext(nextRequest);
                    }
                }
            }
            
            @Override
            public void onError(Throwable t) {
                logger.error("Pub/Sub subscription error: {}", t.getMessage(), t);
                // Implement reconnection logic here
                try {
                    Thread.sleep(5000);
                    if (isSubscribed) {
                        subscriptionLoop(metadata); // Retry subscription
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            
            @Override
            public void onCompleted() {
                logger.info("Pub/Sub subscription completed");
            }
        });
        
        // Store reference for inner class access
        requestObserverRef[0] = requestObserver;
        
        // Send initial subscription request with larger fetch size
        FetchRequest initialRequest = FetchRequest.newBuilder()
                .setTopicName(pubSubTopic)
                .setReplayPreset(ReplayPreset.LATEST)
                .setNumRequested(fetchSize)
                .build();
        
        requestObserver.onNext(initialRequest);
        
        // Keep the subscription alive
        while (isSubscribed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        requestObserver.onCompleted();
    }
    
    /**
     * Cache for Avro schemas to avoid repeated GetSchema calls
     */
    private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    
    /**
     * Get Avro schema by schema ID using Salesforce Pub/Sub API
     */
    private Schema getSchemaById(String schemaId) {
        // Check cache first
        if (schemaCache.containsKey(schemaId)) {
            return schemaCache.get(schemaId);
        }
        
        try {
            // Create blocking stub for schema retrieval
            PubSubGrpc.PubSubBlockingStub blockingStub = PubSubGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(getAuthMetadata()));
            
            // Make GetSchema RPC call
            SchemaRequest schemaRequest = SchemaRequest.newBuilder()
                    .setSchemaId(schemaId)
                    .build();
            
            SchemaInfo schemaInfo = blockingStub.getSchema(schemaRequest);
            
            // Parse the Avro schema JSON
            Schema avroSchema = new Schema.Parser().parse(schemaInfo.getSchemaJson());
            
            // Cache the schema
            schemaCache.put(schemaId, avroSchema);
            
            logger.debug("Retrieved and cached schema for ID: {}", schemaId);
            return avroSchema;
            
        } catch (Exception e) {
            logger.error("Error retrieving schema for ID {}: {}", schemaId, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Get authentication metadata for gRPC calls
     */
    private Metadata getAuthMetadata() {
        try {
            PartnerConnection connection = salesforceClient.getConnections().values().iterator().next();
            String accessToken = connection.getSessionHeader().getSessionId();
            String soapEndpoint = connection.getConfig().getServiceEndpoint();
            URI soapUri = URI.create(soapEndpoint);
            String baseInstanceUrl = soapUri.getScheme() + "://" + soapUri.getHost();
            String tenantId = salesforceClient.getOrgId();
            if (tenantId == null) {
                tenantId = extractTenantId(soapUri.getHost());
            }
            
            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of("accessToken", Metadata.ASCII_STRING_MARSHALLER), accessToken);
            metadata.put(Metadata.Key.of("instanceUrl", Metadata.ASCII_STRING_MARSHALLER), baseInstanceUrl);
            metadata.put(Metadata.Key.of("tenantId", Metadata.ASCII_STRING_MARSHALLER), tenantId);
            return metadata;
        } catch (Exception e) {
            logger.error("Error creating auth metadata: {}", e.getMessage(), e);
            return new Metadata();
        }
    }

    /**
     * Process Avro event and convert to CDC event for batching
     */
    public ChangeDataCaptureEvent processAvroEvent(byte[] avroData, Schema schema) {
        try {
            // Deserialize Avro data
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);
            GenericRecord record = reader.read(null, decoder);
            
            logger.debug("Received Avro event: {}", record.toString());
            
            // Convert Avro record to CDC event for batching
            return convertAvroToCDCEvent(record);
            
        } catch (Exception e) {
            logger.error("Error processing Avro event: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Safely convert Avro object to String, handling Utf8 objects
     */
    private String avroToString(Object avroObj) {
        if (avroObj == null) {
            return null;
        }
        return avroObj.toString();
    }

    /**
     * Convert Avro GenericRecord to CDC event structure for processing
     */
    private ChangeDataCaptureEvent convertAvroToCDCEvent(GenericRecord avroRecord) throws Exception {
        // Extract CDC event data from Avro record
        // The structure will depend on the actual Avro schema from Salesforce
        
        // Create a CDC event structure
        ChangeDataCaptureEvent cdcEvent = new ChangeDataCaptureEvent();
        cdcEvent.ChangeEventHeader = new ChangeEventHeader();
        
        // Extract header information from Avro record
        GenericRecord header = (GenericRecord) avroRecord.get("ChangeEventHeader");
        if (header != null) {
            cdcEvent.ChangeEventHeader.entityName = avroToString(header.get("entityName"));
            cdcEvent.ChangeEventHeader.changeType = avroToString(header.get("changeType"));
            cdcEvent.ChangeEventHeader.transactionKey = avroToString(header.get("transactionKey"));
            cdcEvent.ChangeEventHeader.sequenceNumber = (Integer) header.get("sequenceNumber");
            cdcEvent.ChangeEventHeader.commitTimestamp = (Long) header.get("commitTimestamp");
            cdcEvent.ChangeEventHeader.commitNumber = (Long) header.get("commitNumber");
            cdcEvent.ChangeEventHeader.commitUser = avroToString(header.get("commitUser"));
            
            // Convert recordIds array
            Object recordIdsObj = header.get("recordIds");
            if (recordIdsObj instanceof java.util.List) {
                java.util.List<?> recordIdList = (java.util.List<?>) recordIdsObj;
                cdcEvent.ChangeEventHeader.recordIds = recordIdList.stream()
                    .map(this::avroToString)
                    .toArray(String[]::new);
            }
            
            // Convert field arrays if present
            Object changedFieldsObj = header.get("changedFields");
            if (changedFieldsObj instanceof java.util.List) {
                java.util.List<?> changedFieldsList = (java.util.List<?>) changedFieldsObj;
                cdcEvent.ChangeEventHeader.changedFields = changedFieldsList.stream()
                    .map(this::avroToString)
                    .toArray(String[]::new);
            }
        }
        
        return cdcEvent;
    }
    
    /**
     * Extract tenant ID from Salesforce instance URL
     */
    private String extractTenantId(String host) {
        // Extract from URLs like: mycompany.my.salesforce.com or mycompany--sandbox.my.salesforce.com
        if (host.contains(".my.salesforce.com")) {
            return host.substring(0, host.indexOf(".my.salesforce.com"));
        }
        // Fallback for other formats
        return host.split("\\.")[0];
    }
    
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Pub/Sub subscription and batch processor...");
        isSubscribed = false;
        batchProcessorRunning = false;
        
        // Cancel any pending timeout tasks
        cancelTimeoutTask();
        
        // Shutdown batch processor
        if (batchProcessor != null && !batchProcessor.isShutdown()) {
            try {
                batchProcessor.shutdown();
                if (!batchProcessor.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.warn("Batch processor did not terminate within 10 seconds, forcing shutdown");
                    batchProcessor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                batchProcessor.shutdownNow();
            }
        }
        
        // Process any remaining events in queue
        if (!eventQueue.isEmpty()) {
            logger.info("Processing {} remaining events in queue during shutdown", eventQueue.size());
            List<ChangeDataCaptureEvent> remainingEvents = new ArrayList<>();
            eventQueue.drainTo(remainingEvents);
            if (!remainingEvents.isEmpty()) {
                processBatch(remainingEvents);
            }
        }
        
        // Shutdown gRPC channel
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
        
        logger.info("Pub/Sub subscription and batch processor shutdown complete");
    }
    

} 
