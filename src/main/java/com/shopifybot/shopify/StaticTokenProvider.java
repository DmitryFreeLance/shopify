package com.shopifybot.shopify;

import java.io.IOException;

public class StaticTokenProvider implements TokenProvider {
    private final String token;

    public StaticTokenProvider(String token) {
        this.token = token;
    }

    @Override
    public String getToken() throws IOException {
        return token;
    }
}
