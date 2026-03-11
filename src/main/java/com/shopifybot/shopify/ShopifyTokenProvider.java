package com.shopifybot.shopify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.time.Instant;

public class ShopifyTokenProvider implements TokenProvider {
    private static final MediaType FORM = MediaType.parse("application/x-www-form-urlencoded");

    private final OkHttpClient http;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String shopDomain;
    private final String clientId;
    private final String clientSecret;

    private volatile String accessToken;
    private volatile long expiresAtEpoch;

    public ShopifyTokenProvider(OkHttpClient http, String shopDomain, String clientId, String clientSecret) {
        this.http = http;
        this.shopDomain = shopDomain;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    @Override
    public String getToken() throws IOException {
        if (accessToken != null && Instant.now().getEpochSecond() < expiresAtEpoch) {
            return accessToken;
        }
        synchronized (this) {
            if (accessToken != null && Instant.now().getEpochSecond() < expiresAtEpoch) {
                return accessToken;
            }
            refresh();
            return accessToken;
        }
    }

    private void refresh() throws IOException {
        String body = "grant_type=client_credentials" +
                "&client_id=" + clientId +
                "&client_secret=" + clientSecret;

        Request request = new Request.Builder()
                .url("https://" + shopDomain + "/admin/oauth/access_token")
                .post(RequestBody.create(body, FORM))
                .build();

        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Shopify token request failed: " + response.code() + " " + response.message());
            }
            JsonNode root = mapper.readTree(response.body().string());
            String token = root.path("access_token").asText();
            long expiresIn = root.path("expires_in").asLong(0);
            if (token == null || token.isBlank()) {
                throw new IOException("Shopify token response missing access_token");
            }
            long now = Instant.now().getEpochSecond();
            long safeTtl = Math.max(60, expiresIn - 60);
            this.accessToken = token;
            this.expiresAtEpoch = now + safeTtl;
        }
    }
}
