package com.heroku.java.config;

import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import org.springframework.lang.NonNull;
import reactor.core.publisher.Mono;

@Component
public class ModifyRequestHeadersFilter implements WebFilter {

    @Override
    public @NonNull Mono<Void> filter(@NonNull ServerWebExchange exchange, @NonNull  WebFilterChain chain) {
        // Currently Heroku Eventing pilot is not sending this header and CloudEvents SDK requires it
        ServerHttpRequest originalRequest = exchange.getRequest();
        ServerHttpRequest modifiedRequest = originalRequest.mutate()
                .header("Content-Type", "application/cloudevents+json")
                .build();
        ServerWebExchange modifiedExchange = exchange.mutate()
                .request(modifiedRequest)
                .build();
        return chain.filter(modifiedExchange);
    }
}
