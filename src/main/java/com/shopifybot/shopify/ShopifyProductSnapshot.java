package com.shopifybot.shopify;

public class ShopifyProductSnapshot {
    public final long productId;
    public final long variantId;
    public final String variantBarcode;
    public final String title;
    public final String bodyHtml;
    public final String tags;
    public final String productType;

    public ShopifyProductSnapshot(long productId, long variantId, String variantBarcode, String title, String bodyHtml, String tags, String productType) {
        this.productId = productId;
        this.variantId = variantId;
        this.variantBarcode = variantBarcode;
        this.title = title;
        this.bodyHtml = bodyHtml;
        this.tags = tags;
        this.productType = productType;
    }
}
