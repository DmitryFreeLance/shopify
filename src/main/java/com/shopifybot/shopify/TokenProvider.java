package com.shopifybot.shopify;

import java.io.IOException;

public interface TokenProvider {
    String getToken() throws IOException;
}
