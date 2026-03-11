package com.shopifybot.shopify;

public class ShopifyProductSnapshot {
    public final long productId;
    public final long variantId;
    public final String title;
    public final String tags;
    public final String productType;

    public ShopifyProductSnapshot(long productId, long variantId, String title, String tags, String productType) {
        this.productId = productId;
        this.variantId = variantId;
        this.title = title;
        this.tags = tags;
        this.productType = productType;
    }
}
