package com.heroku.java.services;

import com.heroku.java.config.HerokuEventsClient;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.bind.XmlObject;

import jakarta.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Handles messages sent to the quoteQueue channel to start Quote generation jobs
 */
@Service
@Profile("worker")
public class PricingEngineWorkerService implements MessageListener {

    private static final Logger logger = LoggerFactory.getLogger(PricingEngineWorkerService.class);

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    @Autowired
    private HerokuEventsClient herokuEventsClient;

    @PostConstruct
    public void subscribeToRedisQueue() throws InterruptedException {
        logger.info("Worker subscribing to Redis queue: quoteQueue");
        // Create and configure Redis listener container
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory);
        container.addMessageListener(new MessageListenerAdapter(this, "onMessage"), new PatternTopic("quoteQueue"));
        container.afterPropertiesSet();
        container.start();
        logger.info("Worker is now listening for messages.");        
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        // Extract job ID and SOQL WHERE clause
        String[] messageParts = new String(message.getBody()).split(":", 2);
        if (messageParts.length != 2) {
            logger.error("Invalid message format received: {}", message);
            return;
        }
        String jobId = messageParts[0];
        String recordIds = messageParts[1];
        logger.info("Worker received job with ID: {} for SOQL WHERE clause: {}", jobId, recordIds);
        executeBatch(jobId, recordIds);
    }

    /**
     * Queries for Opportunties, calculates pricing and creates Quotes
     * @param jobId
     * @param soqlWhereClause
     */
    private void executeBatch(String jobId, String recordIds) {
        logger.info("Worker executing batch for Job ID: {} with WHERE clause: {}", jobId, recordIds.toString());

        try {
            // Recreate Salesforce connection
            String sessionId = redis.opsForValue().get("salesforce:session:" + jobId);
            String instanceUrl = redis.opsForValue().get("salesforce:instance:" + jobId);
            PartnerConnection connection = createSalesforceConnection(sessionId, instanceUrl);
            if (connection == null) {
                logger.error("Failed to reconnect to Salesforce for Job ID: {}", jobId);
                return;
            }

            // Generate WHERE clause from the recordIds provided
            String soqlWhereClause = "Id IN (";
            if (recordIds!= null &&!recordIds.isEmpty()) {
                soqlWhereClause += Arrays.stream(recordIds.split(","))
                      .map(id -> "'" + id.trim().replace("'", "\\'") + "'") // Trim, escape, and quote
                      .collect(Collectors.joining(","));
            }
            soqlWhereClause += ")";

            // Fetch Opportunities and related OpportunityLineItems in one SOQL query
            String soql = String.format(
                "SELECT Id, (SELECT Id, Product2Id, Quantity, UnitPrice, PricebookEntryId FROM OpportunityLineItems) " +
                "FROM Opportunity WHERE %s", soqlWhereClause);
            List<SObject> opportunties = queryAllRecords(connection, soql);
            if (opportunties.size() == 0) {
                logger.warn("No Opportunities or related OpportunityLineItems found for WHERE clause: {}", soqlWhereClause);
                return;
            }
            logger.info("Processing {} Opportunities", opportunties.size());

            // Step 1: Collect all Opportunities and related OpportunityLineItems
            List<SObject> quotesToCreate = new ArrayList<>();
            Map<String, XmlObject> opportunityLineItemsMap = new HashMap<>();
            List<String> opportunityIds = new ArrayList<>();
            for (SObject opportunity : opportunties) {
                String opportunityId = opportunity.getId();
                opportunityIds.add(opportunityId);
                // Create Quote record
                SObject quote = new SObject("Quote");
                quote.setField("Name", "New Quote");
                quote.setField("OpportunityId", opportunityId);
                quotesToCreate.add(quote);
                // Store related OpportunityLineItems for processing later
                XmlObject opportunityLineItems = (XmlObject) opportunity.getChild("OpportunityLineItems");
                if (opportunityLineItems != null) {
                    opportunityLineItemsMap.put(opportunityId, opportunityLineItems);
                }
            }
            if (quotesToCreate.isEmpty()) {
                logger.warn("No Quotes to create. Exiting job.");
                return;
            }

            // Step 2: Bulk create Quotes
            logger.info("Performing bulk insert for {} Quotes", quotesToCreate.size());
            List<SaveResult> quoteSaveResults = createParallel(connection, quotesToCreate);

            // Step 3: Map created Quotes back to their OpportunityId
            Map<String, String> opportunityToQuoteMap = new HashMap<>();
            int idx=0;
            for (SaveResult saveResult : quoteSaveResults) {
                if (saveResult.isSuccess()) {
                    opportunityToQuoteMap.put(opportunityIds.get(idx), saveResult.getId());
                } else {
                    logger.error("Failed to create Quote for Opportunity {}: {}", opportunityIds.get(idx), saveResult.getErrors()[0].getMessage());
                }
                idx++;
            }

            // Step 4: Prepare all QuoteLineItems for batch insert
            List<SObject> quoteLineItemsToCreate = new ArrayList<>();
            for (String opportunityId : opportunityToQuoteMap.keySet()) {
                String quoteId = opportunityToQuoteMap.get(opportunityId);
                XmlObject opportunityLineItems = opportunityLineItemsMap.get(opportunityId);
                double discountRate = getDiscountForRegion("US");            
                if (opportunityLineItems == null || !opportunityLineItems.hasChildren()) continue;            
                for (Iterator<XmlObject> it = opportunityLineItems.getChildren(); it.hasNext(); ) {
                    XmlObject child = it.next(); 
                    if(!child.getName().getLocalPart().equals("records")) continue;
                    double quantity = Double.parseDouble(child.getField("Quantity").toString());
                    double unitPrice = Double.parseDouble(child.getField("UnitPrice").toString());
                    double discountedPrice = (quantity * unitPrice) * (1 - discountRate);
                    SObject quoteLineItem = new SObject("QuoteLineItem");
                    quoteLineItem.setField("QuoteId", quoteId);
                    quoteLineItem.setField("PricebookEntryId", child.getField("PricebookEntryId"));
                    quoteLineItem.setField("Quantity", quantity);
                    quoteLineItem.setField("UnitPrice", discountedPrice / quantity);
                    quoteLineItemsToCreate.add(quoteLineItem);
                }
            }
            
            // Step 5: Bulk create QuoteLineItems
            if (!quoteLineItemsToCreate.isEmpty()) {
                logger.info("Performing bulk insert for {} QuoteLineItems", quoteLineItemsToCreate.size());
                List<SaveResult> quoteLineSaveResults = createParallel(connection, quoteLineItemsToCreate);
                for (SaveResult saveResult : quoteLineSaveResults) {
                    if (!saveResult.isSuccess()) {
                        logger.error("Failed to create QuoteLineItem: {}", saveResult.getErrors()[0].getMessage());
                    }
                }
            }

            // Step 6: Send notificaiton back to the Salesforce Org            
            Map<String, Object> payload = new HashMap<>();
            payload.put("Status__c", Map.of("string", String.format("Quotes Generated: %s", quoteSaveResults.size())));
            payload.put("CreatedById", connection.getUserInfo().getUserId());
            payload.put("CreatedDate", System.currentTimeMillis());        
            herokuEventsClient.publish(payload, "QuoteGenerationComplete");

            logger.info("Job processing completed for Job ID: {}", jobId);

        } catch (Exception e) {
            logger.error("Error executing batch: {}", e.toString(), e);
        }
    }

    /**
     * Creates Salesforce WSC connection
     * @param sessionId
     * @param instanceUrl
     * @return
     */
    private PartnerConnection createSalesforceConnection(String sessionId, String instanceUrl) {
        try {
            ConnectorConfig config = new ConnectorConfig();
            config.setServiceEndpoint(instanceUrl);
            config.setSessionId(sessionId);
            return Connector.newConnection(config);
        } catch (Exception e) {
            logger.error("Error creating Salesforce connection: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Ensures all records are retrieved by using query more
     * @param connection
     * @param soql
     * @return
     * @throws ConnectionException
     */
    private List<SObject> queryAllRecords(PartnerConnection connection, String soql) throws ConnectionException {
        List<SObject> allRecords = new ArrayList<>();        
        QueryResult queryResult = connection.query(soql);
        while (queryResult != null) {
            SObject[] records = queryResult.getRecords();
            if (records != null) {
                allRecords.addAll(Arrays.asList(records)); // Use addAll() directly
            }
            if (queryResult.isDone()) {
                break;
            }
            queryResult = connection.queryMore(queryResult.getQueryLocator());
        }
        return allRecords;
    }

    /**
     * Splits SObject creation into parallel requests of 200 or less to accomodate REST API create limit
     * @param connection
     * @param records
     * @return
     */
    private List<SaveResult> createParallel(PartnerConnection connection, List<SObject> records) {        
        ExecutorService executor = Executors.newFixedThreadPool(20);
        List<Future<SaveResult[]>> futures = new ArrayList<>();
        for (int i = 0; i < records.size(); i += 200) {
            int end = Math.min(i + 200, records.size());
            List<SObject> batch = records.subList(i, end);
            logger.info("Creating records from index {} to {} ({} records)", i, end - 1, (end - i));
            Future<SaveResult[]> future = executor.submit(() -> {
                try {  
                    return connection.create(batch.toArray(new SObject[0]));
                } catch (Exception e) {
                    logger.error("Error creating batch: {}", e.getMessage(), e);
                    return new SaveResult[0];
                }
            });
            futures.add(future);
        }
        List<SaveResult> allResults = new ArrayList<>();
        for (Future<SaveResult[]> future : futures) {
            try {
                SaveResult[] batchResults = future.get();
                Collections.addAll(allResults, batchResults);
            } catch (Exception e) {
                logger.error("Error retrieving batch results: {}", e.getMessage(), e);
            }
        }
        executor.shutdown();
        return allResults;
    }    

    /**
     * Sample discount matrix data
     * @param region
     * @return
     */
    private double getDiscountForRegion(String region) {
        // Simple hardcoded discount logic
        switch (region) {
            case "US": return 0.10;
            case "EU": return 0.15;
            case "APAC": return 0.05;
            default: return 0.0;
        }
    }
}
