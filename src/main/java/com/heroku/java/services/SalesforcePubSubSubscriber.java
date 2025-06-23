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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Service that subscribes to Salesforce Change Data Capture events via Pub/Sub API
 * using gRPC and processes them through the existing PricingEngineService
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
    
    @Autowired
    private SalesforceClient salesforceClient;
    
    @Autowired
    private PricingEngineService pricingEngineService;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    private ManagedChannel channel;
    private volatile boolean isSubscribed = false;
    
    /**
     * Start subscription when application is ready
     */
    @EventListener(ApplicationReadyEvent.class)
    public void startSubscription() {
        if (shouldStartSubscription()) {
            logger.info("Starting Salesforce Pub/Sub subscription...");
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
            
            // TODO: Implement actual gRPC subscription using Salesforce Pub/Sub API protobuf definitions
            // This is a placeholder for the actual implementation
                         logger.info("Pub/Sub subscription setup initiated for topic: {}", pubSubTopic);
            isSubscribed = true;
            
            // Simulate subscription loop (replace with actual gRPC streaming call)
            subscriptionLoop(metadata);
            
        } catch (Exception e) {
            logger.error("Error setting up Pub/Sub subscription: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Main subscription loop using actual Salesforce Pub/Sub API gRPC streaming
     */
    private void subscriptionLoop(Metadata metadata) {
        logger.info("Starting subscription loop for Opportunity CDC events");
        
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
                
                // Process each event
                for (ConsumerEvent consumerEvent : response.getEventsList()) {
                    try {
                        // Extract Avro payload from the event
                        byte[] avroData = consumerEvent.getEvent().getPayload().toByteArray();
                        String schemaId = consumerEvent.getEvent().getSchemaId();
                        
                        // Get the schema for this schema ID
                        Schema avroSchema = getSchemaById(schemaId);
                        if (avroSchema != null) {
                            processAvroEvent(avroData, avroSchema);
                        } else {
                            logger.error("Could not retrieve schema for ID: {}", schemaId);
                        }
                        
                    } catch (Exception e) {
                        logger.error("Error processing consumer event: {}", e.getMessage(), e);
                    }
                }
                
                // Request more events
                if (isSubscribed && response.getPendingNumRequested() == 0) {
                    FetchRequest nextRequest = FetchRequest.newBuilder()
                            .setNumRequested(10)
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
        
        // Send initial subscription request
        FetchRequest initialRequest = FetchRequest.newBuilder()
                .setTopicName(pubSubTopic)
                .setReplayPreset(ReplayPreset.LATEST)
                .setNumRequested(10)
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
     * Process Avro event and convert to CDC event for existing processing logic
     */
    public void processAvroEvent(byte[] avroData, Schema schema) {
        try {
            // Deserialize Avro data
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);
            GenericRecord record = reader.read(null, decoder);
            
            logger.info("Received Avro event: {}", record.toString());
            
            // Convert Avro record to CDC event for direct processing
            ChangeDataCaptureEvent cdcEvent = convertAvroToCDCEvent(record);
            
            // Process through existing logic
            pricingEngineService.processChangeDataCaptureEvent(cdcEvent);
            
        } catch (Exception e) {
            logger.error("Error processing Avro event: {}", e.getMessage(), e);
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
        logger.info("Shutting down Pub/Sub subscription...");
        isSubscribed = false;
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }
    

} 
