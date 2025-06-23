# Salesforce Pub/Sub API Implementation Guide

This guide explains how to use direct Salesforce Pub/Sub API integration with Heroku AppLink for authentication to process Change Data Capture events.

## Overview

**Architecture:**
```
Salesforce CDC → Pub/Sub gRPC → Avro Processing → CDC Event Processing
                     ↑
            Auth via Heroku AppLink (SalesforceClient)
```

**Deployment Structure:**
- **Web Dyno**: Runs web server (for Heroku requirements) + Pub/Sub subscriber
- **Worker Dyno**: Processes background jobs from Redis queue
- **HTTP Endpoints**: Minimal health/admin endpoints (not for CDC processing)

## Key Benefits

1. **Direct Connection**: No intermediate addon required
2. **Real-time Performance**: Direct gRPC streaming with low latency
3. **Better Control**: Direct control over subscription parameters (replay, flow control)
4. **Simplified Auth**: Uses Heroku AppLink for authentication
5. **Clean Architecture**: Single approach focused on Pub/Sub API

## Changes Made

### 1. Dependencies Added
- gRPC libraries for Salesforce Pub/Sub API
- Apache Avro for event deserialization
- Protobuf compiler for gRPC stub generation

### 2. New Service: `SalesforcePubSubSubscriber`
- Handles gRPC subscription to Salesforce Pub/Sub API
- Uses existing `SalesforceClient` for authentication
- Converts Avro events to CDC events for direct processing
- Configurable via application properties

### 3. Configuration Properties
```properties
# Enable/disable Pub/Sub subscription
salesforce.pubsub.enabled=${SALESFORCE_PUBSUB_ENABLED:true}

# Pub/Sub API connection settings
salesforce.pubsub.host=${SALESFORCE_PUBSUB_HOST:api.pubsub.salesforce.com}
salesforce.pubsub.port=${SALESFORCE_PUBSUB_PORT:7443}

# Topic to subscribe to
salesforce.pubsub.topic=${SALESFORCE_PUBSUB_TOPIC:/data/OpportunityChangeEvent}

# Replay preset (LATEST, EARLIEST, CUSTOM)
salesforce.pubsub.replay-preset=${SALESFORCE_PUBSUB_REPLAY_PRESET:LATEST}
```

## Implementation Status

### ✅ Completed
- Maven dependencies for gRPC and Avro
- `SalesforcePubSubSubscriber` service skeleton
- Configuration properties  
- Integration with existing `SalesforceClient`
- `PricingEngineService` converted to pure service for CDC processing
- `HealthController` added for Heroku deployment requirements
- Avro to CDC event conversion logic
- Shared CDC event model classes

### 🚧 TODO (Complete Implementation)
1. **Add Official Salesforce Pub/Sub Protobuf Definitions**
   - Download official protobuf files from Salesforce
   - Replace the placeholder `pubsub_api.proto` with official definitions

2. **Implement Actual gRPC Subscription**
   - Replace placeholder subscription loop with real gRPC streaming
   - Implement proper error handling and reconnection logic
   - Add schema retrieval and caching

3. **Add Replay ID Management**
   - Store replay IDs for resumable subscriptions
   - Implement proper replay handling

4. **Complete Testing**
   - Test with real Salesforce org
   - Verify CDC events are properly processed
   - Performance testing vs HTTP approach

## Migration Steps

### Option 1: Gradual Migration (Recommended)
1. **Deploy with Pub/Sub Disabled**
   ```bash
   heroku config:set SALESFORCE_PUBSUB_ENABLED=false
   ```

2. **Test HTTP Endpoint Still Works**
   - Verify existing Heroku Eventing continues to work
   - Monitor logs for proper event processing

3. **Enable Pub/Sub Subscription**
   ```bash
   heroku config:set SALESFORCE_PUBSUB_ENABLED=true
   ```

4. **Monitor Both Approaches**
   - Check logs to see events from both sources
   - Verify no duplicate processing

5. **Disable Heroku Eventing**
   - Once confident in Pub/Sub approach
   - Remove Heroku Eventing addon

### Option 2: Direct Migration
1. **Remove Heroku Eventing addon**
2. **Deploy with Pub/Sub enabled**
3. **Monitor for proper event processing**

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SALESFORCE_PUBSUB_ENABLED` | Enable/disable Pub/Sub subscription | `true` |
| `SALESFORCE_PUBSUB_HOST` | Pub/Sub API hostname | `api.pubsub.salesforce.com` |
| `SALESFORCE_PUBSUB_PORT` | Pub/Sub API port | `7443` |
| `SALESFORCE_PUBSUB_TOPIC` | Topic to subscribe to | `/data/OpportunityChangeEvent` |
| `SALESFORCE_PUBSUB_REPLAY_PRESET` | Replay preset | `LATEST` |

## Monitoring

### Log Messages to Watch
```
# Pub/Sub startup
INFO  c.h.j.s.SalesforcePubSubSubscriber - Starting Salesforce Pub/Sub subscription...
INFO  c.h.j.s.SalesforcePubSubSubscriber - Connecting to Salesforce Pub/Sub API for tenant: <tenant>

# Event processing distinction  
INFO  c.h.j.s.PricingEngineService - Processing event from direct Pub/Sub subscription
INFO  c.h.j.s.PricingEngineService - Processing event from HTTP endpoint (Heroku Eventing)
```

### Health Checks
- Monitor gRPC connection status
- Track event processing latency
- Watch for subscription failures and reconnections

## Troubleshooting

### Common Issues
1. **Authentication Failures**
   - Verify Heroku AppLink is properly configured
   - Check Salesforce connection in logs

2. **gRPC Connection Issues**
   - Check network connectivity to `api.pubsub.salesforce.com:7443`
   - Verify TLS/SSL configuration

3. **Event Processing Errors**
   - Check Avro schema compatibility
   - Verify event structure matches expectations

### Debug Settings
```properties
# Enable debug logging
logging.level.com.heroku.java.services.SalesforcePubSubSubscriber=DEBUG
logging.level.io.grpc=DEBUG
```

## Performance Considerations

### gRPC vs HTTP
- **Lower Latency**: Direct gRPC streaming vs HTTP polling
- **Better Throughput**: Binary Avro vs JSON CloudEvents
- **Flow Control**: Can specify number of events to receive

### Resource Usage
- **Memory**: Avro deserialization requires schema caching
- **CPU**: gRPC processing is more efficient than HTTP
- **Network**: HTTP/2 multiplexing vs HTTP/1.1

## Next Steps

1. **Complete the TODO items above**
2. **Test with a development Salesforce org**
3. **Implement proper error handling and monitoring**
4. **Consider adding metrics/telemetry**
5. **Update operational procedures**

## References

- [Salesforce Pub/Sub API Documentation](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/intro.html)
- [Java Quick Start Guide](https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-java-quick-start.html)
- [Pub/Sub API GitHub Repository](https://github.com/forcedotcom/pub-sub-api) 
