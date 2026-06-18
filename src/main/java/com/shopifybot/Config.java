package com.shopifybot;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class Config {
    public final String telegramBotToken;
    public final String telegramChannelId;
    public final String telegramChannelUsername;
    public final String telegramPublishChatId;
    public final Integer telegramPublishThreadId;
    public final String telegramLinkMetafieldNamespace;
    public final String telegramLinkMetafieldKey;
    public final String telegramLinkMetafieldType;
    public final List<Long> adminUserIds;
    public final int listPageSize;
    public final String priceKeyword;
    public final String priceSource;
    public final List<String> soldKeywords;
    public final long mediaGroupFinalizeSeconds;
    public final long productSyncSeconds;
    public final long productSyncDelayMs;
    public final int productSyncBatchSize;
    public final int productSyncPosOnlyBatchSize;
    public final long discountSyncSeconds;
    public final String discountTimezone;
    public final String sqlitePath;
    public final long shopifyRateLimitCooldownSeconds;

    public final String kieApiKey;
    public final String kieModel;
    public final String kieFallbackModel;
    public final String kieSecondFallbackModel;
    public final String kieBaseUrl;
    public final String kieEndpointOverride;
    public final String kieFallbackEndpointOverride;
    public final String kieSecondFallbackEndpointOverride;
    public final Duration kieTimeout;
    public final int kieRetryAttempts;
    public final long kieRetryDelayMs;

    public final String shopifyShopDomain;
    public final String shopifyAdminToken;
    public final String shopifyClientId;
    public final String shopifyClientSecret;
    public final String shopifyApiVersion;
    public final boolean shopifyPublishAll;

    public final long maxImageBytes;
    public final int maxMediaGroupImages;

    public Config() {
        this.telegramBotToken = requireEnv("TELEGRAM_BOT_TOKEN");
        this.telegramChannelId = getenv("TELEGRAM_CHANNEL_ID", "-1003856584928");
        this.telegramChannelUsername = getenv("TELEGRAM_CHANNEL_USERNAME", "");
        this.telegramPublishChatId = getenv("TELEGRAM_PUBLISH_CHAT_ID", this.telegramChannelId);
        this.telegramPublishThreadId = parseNullableInt(getenv("TELEGRAM_PUBLISH_THREAD_ID", ""));
        this.telegramLinkMetafieldNamespace = getenv("TELEGRAM_LINK_METAFIELD_NAMESPACE", "custom");
        this.telegramLinkMetafieldKey = getenv("TELEGRAM_LINK_METAFIELD_KEY", "tg_link");
        this.telegramLinkMetafieldType = getenv("TELEGRAM_LINK_METAFIELD_TYPE", "link");
        this.adminUserIds = Arrays.stream(getenv("ADMIN_USER_IDS", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Config::parseLongStrict)
                .filter(v -> v != null && v > 0)
                .collect(Collectors.toList());
        this.listPageSize = (int) parseLong(getenv("LIST_PAGE_SIZE", "8"), 8);
        this.priceKeyword = getenv("PRICE_KEYWORD", "cena").toLowerCase(Locale.ROOT);
        this.priceSource = getenv("PRICE_SOURCE", "AUTO");
        this.soldKeywords = Arrays.stream(getenv("SOLD_KEYWORDS", "prodato").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s.toLowerCase(Locale.ROOT))
                .collect(Collectors.toList());
        this.mediaGroupFinalizeSeconds = parseLong(getenv("MEDIA_GROUP_FINALIZE_SECONDS", "30"), 30);
        this.productSyncSeconds = parseLong(getenv("PRODUCT_SYNC_SECONDS", "180"), 180);
        this.productSyncDelayMs = parseLong(getenv("PRODUCT_SYNC_DELAY_MS", "200"), 200);
        this.productSyncBatchSize = (int) parseLong(getenv("PRODUCT_SYNC_BATCH_SIZE", "20"), 20);
        this.productSyncPosOnlyBatchSize = (int) parseLong(getenv("PRODUCT_SYNC_POS_ONLY_BATCH_SIZE", "5"), 5);
        this.discountSyncSeconds = parseLong(getenv("DISCOUNT_SYNC_SECONDS", "3600"), 3600);
        this.discountTimezone = getenv("DISCOUNT_TIMEZONE", "Europe/Belgrade");
        this.sqlitePath = getenv("SQLITE_PATH", "./data/bot.db");
        this.shopifyRateLimitCooldownSeconds = parseLong(getenv("SHOPIFY_RATE_LIMIT_COOLDOWN_SECONDS", "90"), 90);

        this.kieApiKey = requireEnv("KIE_API_KEY");
        this.kieModel = getenv("KIE_MODEL", "gemini-3-flash");
        this.kieFallbackModel = getenv("KIE_FALLBACK_MODEL", "gemini-2.5-flash");
        this.kieSecondFallbackModel = getenv("KIE_SECOND_FALLBACK_MODEL", "");
        this.kieBaseUrl = getenv("KIE_BASE_URL", "https://api.kie.ai");
        this.kieEndpointOverride = getenv("KIE_ENDPOINT", "");
        this.kieFallbackEndpointOverride = getenv("KIE_FALLBACK_ENDPOINT", "");
        this.kieSecondFallbackEndpointOverride = getenv("KIE_SECOND_FALLBACK_ENDPOINT", "");
        this.kieTimeout = Duration.ofSeconds(parseLong(getenv("KIE_TIMEOUT_SECONDS", "180"), 180));
        this.kieRetryAttempts = (int) parseLong(getenv("KIE_RETRY_ATTEMPTS", "10"), 10);
        this.kieRetryDelayMs = parseLong(getenv("KIE_RETRY_DELAY_MS", "1000"), 1000);

        this.shopifyShopDomain = requireEnv("SHOPIFY_SHOP_DOMAIN");
        this.shopifyAdminToken = getenv("SHOPIFY_ADMIN_TOKEN", "");
        this.shopifyClientId = getenv("SHOPIFY_CLIENT_ID", "");
        this.shopifyClientSecret = getenv("SHOPIFY_CLIENT_SECRET", "");
        this.shopifyApiVersion = getenv("SHOPIFY_API_VERSION", "2024-10");
        this.shopifyPublishAll = Boolean.parseBoolean(getenv("SHOPIFY_PUBLISH_ALL", "true"));

        if ((shopifyClientId.isBlank() || shopifyClientSecret.isBlank()) && shopifyAdminToken.isBlank()) {
            throw new IllegalStateException("Missing Shopify auth. Set SHOPIFY_ADMIN_TOKEN or SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET");
        }

        this.maxImageBytes = parseLong(getenv("MAX_IMAGE_BYTES", "4194304"), 4194304);
        this.maxMediaGroupImages = (int) parseLong(getenv("MAX_MEDIA_GROUP_IMAGES", "4"), 4);
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

    private static Integer parseNullableInt(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLongStrict(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
