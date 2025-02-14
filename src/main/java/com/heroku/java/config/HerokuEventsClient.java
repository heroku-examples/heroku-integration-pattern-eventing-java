package com.heroku.java.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import jakarta.annotation.PostConstruct;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

@Component
public class HerokuEventsClient {

    private static final Logger logger = LoggerFactory.getLogger(HerokuEventsClient.class);

    private String publishUrl;
    private String username;
    private String password;

    @PostConstruct
    public void initializeConnection() {
        publishUrl = System.getenv("HEROKUEVENTS_PUBLISH_URL");
        String userInfo = null;
        try {
            URI uri = new URI(publishUrl);
            userInfo = uri.getUserInfo();
        } catch (URISyntaxException e) {
            logger.error("Invalid HEROKUEVENTS_PUBLISH_URL format: {}", e.toString());
        }        
        if (userInfo != null && userInfo.contains(":")) {
            String[] credentials = userInfo.split(":", 2);
            username = credentials[0];
            password = credentials[1];
        } else {
            throw new IllegalArgumentException("HEROKUEVENTS_PUBLISH_URL must include username and password");
        }
    }

    public void publish(Map<String, Object> payload, String publishName) {
            // Create Http request to publish event
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBasicAuth(username, password);
            HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(payload, headers);        
            // Publish to Heroku to send the event
            try {
                String response = restTemplate.exchange(publishUrl + "/" + publishName, HttpMethod.POST, requestEntity, String.class).getBody();
            logger.info("Response from server: {}", response);
        } catch (Exception e) {
            logger.error("Error sending POST request: {}", e.toString());
        }
    }
}
