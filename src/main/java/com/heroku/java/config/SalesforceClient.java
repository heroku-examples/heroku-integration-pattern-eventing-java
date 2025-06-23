package com.heroku.java.config;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Component
public class SalesforceClient {

    @Value("${HEROKU_APPLINK_API_URL:#{null}}")
    private String integrationApiUrl;

    @Value("${HEROKU_APPLINK_TOKEN:#{null}}")
    private String invocationsToken;

    @Value("${CONNECTION_NAMES:#{null}}")
    private String connectionNames;

    @Value("${HEROKU_APP_ID:#{null}}")
    private String herokuAppId;

    // Manual SF CLI authentication (for debugging)
    @Value("${SF_SESSION_ID:#{null}}")
    private String sfSessionId;

    @Value("${SF_INSTANCE_URL:#{null}}")
    private String sfInstanceUrl;

    @Value("${SF_API_VERSION:59.0}")
    private String sfApiVersion;

    @Value("${SF_ORG_ID:#{null}}")
    private String sfOrgId;

    private final Map<String, PartnerConnection> connections = new HashMap<>();

    @PostConstruct
    public void initializeConnections() {
        // Check if manual SF CLI auth is available
        if (sfSessionId != null && sfInstanceUrl != null) {
            System.out.println("Using manual SF CLI authentication for debugging");
            try {
                PartnerConnection connection = createManualConnection();
                // Use the first connection name or default to "debug"
                String connectionName = "debug";
                if (connectionNames != null && !connectionNames.trim().isEmpty()) {
                    connectionName = connectionNames.split(",")[0].trim();
                }
                connections.put(connectionName, connection);
                return;
            } catch (Exception e) {
                System.err.println("Failed to create manual SF connection, falling back to AppLink: " + e.getMessage());
            }
        }

        // Fall back to AppLink authentication
        if (integrationApiUrl == null || invocationsToken == null || connectionNames == null) {
            throw new IllegalStateException("Neither manual SF CLI auth nor Heroku AppLink environment variables are set properly.");
        }
        String[] connectionNameArray = connectionNames.split(",");
        for (String connectionName : connectionNameArray) {
            connectionName = connectionName.trim();
            if (!connectionName.isEmpty()) {
                try {
                    connections.put(connectionName, createConnection(connectionName));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to initialize connection for: " + connectionName, e);
                }
            }
        }
    }

    private PartnerConnection createManualConnection() throws ConnectionException {
        ConnectorConfig config = new ConnectorConfig();
        config.setServiceEndpoint(sfInstanceUrl + "/services/Soap/u/" + sfApiVersion);
        config.setSessionId(sfSessionId);
        return new PartnerConnection(config);
    }

    private PartnerConnection createConnection(String developerName) throws ConnectionException {
        if (developerName == null || developerName.isEmpty()) {
            throw new IllegalArgumentException("Developer name not provided");
        }
        
        // Prepare headers
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + invocationsToken);
        headers.set("X-App-UUID", herokuAppId);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<?> entity = new HttpEntity<>(headers);

        // Make the REST call to get auth details
        String authUrl = integrationApiUrl + "/authorizations/" + developerName;
        ResponseEntity<Map<String, Object>> response = new RestTemplate().exchange(
            authUrl, HttpMethod.GET, entity, new ParameterizedTypeReference<>() {}
        );
        
        if (response.getBody() == null || !response.getBody().containsKey("org")) {
            throw new IllegalStateException("Invalid response from Heroku AppLink API.");
        }

        // Extract org data from nested response structure
        @SuppressWarnings("unchecked")
        Map<String, Object> org = (Map<String, Object>) response.getBody().get("org");
        @SuppressWarnings("unchecked")
        Map<String, Object> userAuth = (Map<String, Object>) org.get("user_auth");

        // Retrieve authentication details
        String accessToken = (String) userAuth.get("access_token");
        String apiVersion = (String) org.get("api_version");
        String orgDomainUrl = (String) org.get("instance_url");

        // Configure and create Salesforce PartnerConnection
        ConnectorConfig config = new ConnectorConfig();
        String cleanApiVersion = apiVersion.replaceFirst("^v", "");
        config.setServiceEndpoint(orgDomainUrl + "/services/Soap/u/" + cleanApiVersion);
        config.setSessionId(accessToken);
        return new PartnerConnection(config);
    }

    public PartnerConnection getConnection(String connectionName) {
        if (!connections.containsKey(connectionName)) {
            throw new IllegalArgumentException("No Salesforce PartnerConnection found for: " + connectionName);
        }
        return connections.get(connectionName);
    }

    public Map<String, PartnerConnection> getConnections() {
        return new HashMap<>(connections);
    }

    public String getOrgId() {
        return sfOrgId;
    }
}
