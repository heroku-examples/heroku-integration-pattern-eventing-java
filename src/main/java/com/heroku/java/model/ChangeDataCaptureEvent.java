package com.heroku.java.model;

/**
 * Represents a Salesforce Change Data Capture event structure
 * Used for processing CDC events from Pub/Sub API
 */
public class ChangeDataCaptureEvent {
    public ChangeEventHeader ChangeEventHeader;
    
    public static class ChangeEventHeader {
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
