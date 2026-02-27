package com.shopifybot;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class Config {
    public final String telegramBotToken;
    public final String telegramChannelId;
    public final String priceKeyword;
    public final List<String> soldKeywords;
    public final long mediaGroupFinalizeSeconds;
    public final long productSyncSeconds;
    public final String sqlitePath;

    public final String kieApiKey;
    public final String kieModel;
    public final String kieFallbackModel;
    public final String kieBaseUrl;
    public final String kieEndpointOverride;
    public final String kieFallbackEndpointOverride;
    public final Duration kieTimeout;

    public final String shopifyShopDomain;
    public final String shopifyAdminToken;
    public final String shopifyApiVersion;
    public final boolean shopifyPublishAll;

    public final long maxImageBytes;

    public Config() {
        this.telegramBotToken = requireEnv("TELEGRAM_BOT_TOKEN");
        this.telegramChannelId = getenv("TELEGRAM_CHANNEL_ID", "-1003856584928");
        this.priceKeyword = getenv("PRICE_KEYWORD", "cena").toLowerCase(Locale.ROOT);
        this.soldKeywords = Arrays.stream(getenv("SOLD_KEYWORDS", "prodato").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s.toLowerCase(Locale.ROOT))
                .collect(Collectors.toList());
        this.mediaGroupFinalizeSeconds = parseLong(getenv("MEDIA_GROUP_FINALIZE_SECONDS", "30"), 30);
        this.productSyncSeconds = parseLong(getenv("PRODUCT_SYNC_SECONDS", "180"), 180);
        this.sqlitePath = getenv("SQLITE_PATH", "./data/bot.db");

        this.kieApiKey = requireEnv("KIE_API_KEY");
        this.kieModel = getenv("KIE_MODEL", "gemini-3-flash");
        this.kieFallbackModel = getenv("KIE_FALLBACK_MODEL", "gpt-5-2");
        this.kieBaseUrl = getenv("KIE_BASE_URL", "https://api.kie.ai");
        this.kieEndpointOverride = getenv("KIE_ENDPOINT", "");
        this.kieFallbackEndpointOverride = getenv("KIE_FALLBACK_ENDPOINT", "");
        this.kieTimeout = Duration.ofSeconds(parseLong(getenv("KIE_TIMEOUT_SECONDS", "90"), 90));

        this.shopifyShopDomain = requireEnv("SHOPIFY_SHOP_DOMAIN");
        this.shopifyAdminToken = requireEnv("SHOPIFY_ADMIN_TOKEN");
        this.shopifyApiVersion = getenv("SHOPIFY_API_VERSION", "2024-10");
        this.shopifyPublishAll = Boolean.parseBoolean(getenv("SHOPIFY_PUBLISH_ALL", "true"));

        this.maxImageBytes = parseLong(getenv("MAX_IMAGE_BYTES", "4194304"), 4194304);
    }

    private static String getenv(String key, String defaultValue) {
        String val = System.getenv(key);
        return val == null ? defaultValue : val.trim();
    }

    private static String requireEnv(String key) {
        String val = System.getenv(key);
        if (val == null || val.trim().isEmpty()) {
            throw new IllegalStateException("Missing required env: " + key);
        }
        return val.trim();
    }

    private static long parseLong(String value, long fallback) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
