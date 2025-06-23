package com.heroku.java.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.heroku.java.config.SalesforceClient;
import com.heroku.java.model.ChangeDataCaptureEvent;
import com.salesforce.eventbus.protobuf.*;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
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

import javax.annotation.PreDestroy;
import javax.net.ssl.SSLException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.sforce.soap.partner.PartnerConnection;

/**
 * Service that subscribes to Salesforce Pub/Sub API for Change Data Capture events
 * Simplified to focus on event reception and forwarding to PricingEngineService for batching
 */
@Service
public class SalesforcePubSubService {

    private static final Logger logger = LoggerFactory.getLogger(SalesforcePubSubService.class);

    @Value("${salesforce.pubsub.host:api.pubsub.salesforce.com}")
    private String pubSubHost;

    @Value("${salesforce.pubsub.port:7443}")
    private int pubSubPort;

    @Value("${salesforce.pubsub.topic:/data/OpportunityChangeEvent}")
    private String pubSubTopic;

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

    /**
     * Cache for Avro schemas to avoid repeated GetSchema calls
     */
    private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();

    /**
     * Start subscription when application is ready
     */
    @EventListener(ApplicationReadyEvent.class)
    public void startSubscription() {
        if (shouldStartSubscription()) {
            logger.info("Starting Salesforce Pub/Sub subscription...");
            subscribeToOpportunityChangeEvents();
        } else {
            logger.info("Pub/Sub subscription disabled for this profile");
        }
    }

    /**
     * Check if subscription should start based on configuration
     */
    private boolean shouldStartSubscription() {
        return !"false".equals(System.getProperty("salesforce.pubsub.enabled", "true"));
    }

    /**
     * Subscribe to Salesforce Pub/Sub API for Opportunity Change Events
     */
    public void subscribeToOpportunityChangeEvents() {
        try {
            // Create gRPC channel with SSL
            channel = NettyChannelBuilder.forAddress(pubSubHost, pubSubPort)
                    .sslContext(GrpcSslContexts.forClient().build())
                    .build();

            // Get authentication metadata
            Metadata metadata = getAuthMetadata();
            
            logger.info("Pub/Sub subscription setup initiated for topic: {}", pubSubTopic);
            logger.info("Starting subscription loop for Opportunity CDC events");
            
            // Start subscription in background thread
            Thread subscriptionThread = new Thread(() -> subscriptionLoop(metadata));
            subscriptionThread.setDaemon(true);
            subscriptionThread.start();
            
        } catch (SSLException e) {
            logger.error("SSL error setting up Pub/Sub connection: {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error setting up Pub/Sub subscription: {}", e.getMessage(), e);
        }
    }

    /**
     * Main subscription loop - simplified to process events individually
     */
    private void subscriptionLoop(Metadata metadata) {
        isSubscribed = true;
        
        // Create stub with authentication
        PubSubGrpc.PubSubStub pubSubStub = PubSubGrpc.newStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
        
        // Store reference for inner class access
        @SuppressWarnings("unchecked")
        final StreamObserver<FetchRequest>[] requestObserverRef = (StreamObserver<FetchRequest>[]) new StreamObserver[1];
        
        // Create bidirectional streaming call
        StreamObserver<FetchRequest> requestObserver = pubSubStub.subscribe(new StreamObserver<FetchResponse>() {
            @Override
            public void onNext(FetchResponse response) {
                logger.info("Received {} events from Pub/Sub API", response.getEventsCount());
                
                // Process each event individually - let PricingEngineService handle batching
                for (ConsumerEvent consumerEvent : response.getEventsList()) {
                    try {
                        // Extract Avro payload from the event
                        byte[] avroData = consumerEvent.getEvent().getPayload().toByteArray();
                        String schemaId = consumerEvent.getEvent().getSchemaId();
                        
                        // Get the schema for this schema ID
                        Schema avroSchema = getSchemaById(schemaId);
                        if (avroSchema != null) {
                            // Convert to CDC event and pass to PricingEngineService
                            ChangeDataCaptureEvent cdcEvent = processAvroEvent(avroData, avroSchema);
                            if (cdcEvent != null) {
                                logger.debug("Processing CDC event: {}", cdcEvent.ChangeEventHeader.transactionKey);
                                // Let PricingEngineService handle batching logic
                                pricingEngineService.generateQuote(cdcEvent);
                            }
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
        
        // Send initial subscription request
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
     * Extract tenant ID from Salesforce host
     */
    private String extractTenantId(String host) {
        // Extract org ID from hostname like "mycompany--sandbox.my.salesforce.com"
        if (host.contains(".my.salesforce.com")) {
            return host.split("\\.")[0];
        }
        return "unknown";
    }

    /**
     * Process Avro event and convert to CDC event
     */
    public ChangeDataCaptureEvent processAvroEvent(byte[] avroData, Schema schema) {
        try {
            // Deserialize Avro data
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);
            GenericRecord record = reader.read(null, decoder);
            
            logger.debug("Received Avro event: {}", record.toString());
            
            // Convert Avro record to CDC event
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
     * Convert Avro GenericRecord to CDC event structure
     */
    private ChangeDataCaptureEvent convertAvroToCDCEvent(GenericRecord avroRecord) throws Exception {
        // Extract CDC event data from Avro record
        ChangeDataCaptureEvent cdcEvent = new ChangeDataCaptureEvent();
        
        // Extract ChangeEventHeader
        GenericRecord headerRecord = (GenericRecord) avroRecord.get("ChangeEventHeader");
        if (headerRecord != null) {
            ChangeDataCaptureEvent.ChangeEventHeader header = new ChangeDataCaptureEvent.ChangeEventHeader();
            header.entityName = avroToString(headerRecord.get("entityName"));
            header.changeType = avroToString(headerRecord.get("changeType"));
            header.transactionKey = avroToString(headerRecord.get("transactionKey"));
            
            // Handle recordIds array
            Object recordIdsObj = headerRecord.get("recordIds");
            if (recordIdsObj instanceof org.apache.avro.generic.GenericData.Array) {
                @SuppressWarnings("unchecked")
                org.apache.avro.generic.GenericData.Array<Object> recordIdsArray = 
                    (org.apache.avro.generic.GenericData.Array<Object>) recordIdsObj;
                header.recordIds = recordIdsArray.stream()
                    .map(this::avroToString)
                    .toArray(String[]::new);
            }
            
            cdcEvent.ChangeEventHeader = header;
        }
        
        return cdcEvent;
    }

    /**
     * Shutdown hook to clean up resources
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Pub/Sub subscription...");
        isSubscribed = false;
        
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
        }
        
        logger.info("Pub/Sub subscription shutdown complete");
    }
} 
