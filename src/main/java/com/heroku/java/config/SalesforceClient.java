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

    @Value("${HEROKU_INTEGRATION_API_URL:#{null}}")
    private String integrationApiUrl;

    @Value("${HEROKU_INTEGRATION_TOKEN:#{null}}")
    private String invocationsToken;

    @Value("${CONNECTION_NAMES:#{null}}")
    private String connectionNames;

    private final Map<String, PartnerConnection> connections = new HashMap<>();

    @PostConstruct
    public void initializeConnections() {
        if (integrationApiUrl == null || invocationsToken == null || connectionNames == null) {
            throw new IllegalStateException("Heroku Integration environment variables are not set properly.");
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

    private PartnerConnection createConnection(String connectionName) throws ConnectionException {
        // Prepare request payload
        Map<String, String> requestBody = Map.of("org_name", connectionName);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + invocationsToken);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, String>> entity = new HttpEntity<>(requestBody, headers);

        // Make the REST call to get auth details
        String authUrl = integrationApiUrl + "/invocations/authorization";
        ResponseEntity<Map<String, Object>> response = new RestTemplate().exchange(
            authUrl, HttpMethod.POST, entity, new ParameterizedTypeReference<>() {}
        );
        if (response.getBody() == null || !response.getBody().containsKey("access_token")) {
            throw new IllegalStateException("Invalid response from Salesforce Integration API.");
        }

        // Retrieve authentication details
        String accessToken = (String) response.getBody().get("access_token");
        String apiVersion = (String) response.getBody().get("api_version");
        String orgDomainUrl = (String) response.getBody().get("org_domain_url");

        // Configure and create Salesforce PartnerConnection
        ConnectorConfig config = new ConnectorConfig();
        config.setServiceEndpoint(orgDomainUrl + "/services/Soap/u/" + apiVersion.replaceFirst("^v", ""));
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
}
