package com.shopifybot.shopify;

public class ShopifyProductSnapshot {
    public final long productId;
    public final long variantId;
    public final long inventoryItemId;
    public final String variantBarcode;
    public final String variantSku;
    public final String title;
    public final String bodyHtml;
    public final String tags;
    public final String productType;

    public ShopifyProductSnapshot(long productId, long variantId, long inventoryItemId, String variantBarcode, String variantSku, String title, String bodyHtml, String tags, String productType) {
        this.productId = productId;
        this.variantId = variantId;
        this.inventoryItemId = inventoryItemId;
        this.variantBarcode = variantBarcode;
        this.variantSku = variantSku;
        this.title = title;
        this.bodyHtml = bodyHtml;
        this.tags = tags;
        this.productType = productType;
    }
}
