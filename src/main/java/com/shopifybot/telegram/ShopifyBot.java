package com.shopifybot.telegram;

import com.shopifybot.Config;
import com.shopifybot.ai.CategorySelection;
import com.shopifybot.ai.Classification;
import com.shopifybot.ai.KieAiClient;
import com.shopifybot.db.Database;
import com.shopifybot.db.Database.PostRef;
import com.shopifybot.shopify.ShopifyClient;
import com.shopifybot.shopify.ShopifyCollections;
import com.shopifybot.shopify.ShopifyProductSnapshot;
import com.shopifybot.shopify.TokenProvider;
import com.shopifybot.util.TextParser;
import okhttp3.OkHttpClient;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.DeleteMessage;
import org.telegram.telegrambots.meta.api.methods.GetFile;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.PhotoSize;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShopifyBot extends TelegramLongPollingBot {
    private static final Logger log = LoggerFactory.getLogger(ShopifyBot.class);
    private final Config config;
    private final Database db;
    private final ShopifyClient shopify;
    private final ShopifyCollections collections;
    private final KieAiClient kie;
    private String shopCurrency;

    private final ExecutorService workers = Executors.newFixedThreadPool(2);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public ShopifyBot(Config config, Database db, OkHttpClient http, TokenProvider tokenProvider) {
        super(config.telegramBotToken);
        this.config = config;
        this.db = db;
        this.shopify = new ShopifyClient(http, config.shopifyShopDomain, config.shopifyApiVersion, tokenProvider);
        this.collections = new ShopifyCollections(shopify, db);
        this.kie = new KieAiClient(http, config.kieApiKey, config.kieModel, config.kieBaseUrl, config.kieEndpointOverride);

        scheduler.scheduleWithFixedDelay(this::processReadyMediaGroups, 15, 15, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::syncDeletedProducts, config.productSyncSeconds, config.productSyncSeconds, TimeUnit.SECONDS);
    }

    public void initialize() {
        try {
            collections.ensureCollections();
            shopCurrency = shopify.getShopCurrency();
            if (shopCurrency == null || shopCurrency.isBlank()) {
                shopCurrency = "UNKNOWN";
            }
            log.info("Shop currency detected: {}", shopCurrency);
            log.info("Collections ensured");
        } catch (IOException e) {
            throw new RuntimeException("Failed to ensure Shopify collections", e);
        }
    }

    @Override
    public void onUpdateReceived(Update update) {
        log.info(
                "Update received: message={}, edited_message={}, channel_post={}, edited_channel_post={}, my_chat_member={}, chat_member={}",
                update.hasMessage(),
                update.hasEditedMessage(),
                update.hasChannelPost(),
                update.hasEditedChannelPost(),
                update.hasMyChatMember(),
                update.hasChatMember()
        );
        if (update.hasChannelPost()) {
            handleMessage(update.getChannelPost(), false);
        }
        if (update.hasEditedChannelPost()) {
            handleMessage(update.getEditedChannelPost(), true);
        }
        if (update.hasMessage()) {
            handleMessage(update.getMessage(), false);
        }
        if (update.hasEditedMessage()) {
            handleMessage(update.getEditedMessage(), true);
        }
    }

    private void handleMessage(Message message, boolean isEdit) {
        String channelId = String.valueOf(message.getChatId());
        if (config.telegramChannelId != null && !config.telegramChannelId.isBlank() && !config.telegramChannelId.equals(channelId)) {
            log.info("Ignored message {} from channel {} (expected {})", message.getMessageId(), channelId, config.telegramChannelId);
            return;
        }

        String rawText = extractText(message);
        final String text = rawText == null ? "" : rawText;

        boolean hasPhotos = message.getPhoto() != null && !message.getPhoto().isEmpty();
        log.info("Message {} received. edit={}, mediaGroup={}, hasPhotos={}, textLength={}",
                message.getMessageId(), isEdit, message.getMediaGroupId(), hasPhotos, text.length());

        db.upsertPost(channelId, message.getMessageId(), message.getMediaGroupId(), text, message.getDate());

        if (isEdit) {
            db.updatePostText(channelId, message.getMessageId(), text);
            if (TextParser.containsAnyKeyword(text, config.soldKeywords)) {
                log.info("Message {} marked as sold (keywords={})", message.getMessageId(), config.soldKeywords);
                if (message.getMediaGroupId() != null) {
                    db.markMediaGroupProcessed(message.getMediaGroupId());
                }
                db.markPostStatus(channelId, message.getMessageId(), "SOLD");
                workers.submit(() -> handleSold(channelId, message.getMessageId(), message.getMediaGroupId()));
                return;
            }

            if (message.getMediaGroupId() != null) {
                storeMediaGroupItem(message);
            }

            Long existingProductId = db.findProductId(channelId, message.getMessageId());
            if (existingProductId != null && (TextParser.containsKeyword(text, config.priceKeyword) || TextParser.startsWithSaleHeader(text))) {
                workers.submit(() -> {
                    boolean updated = updateExistingProduct(channelId, message.getMessageId(), message.getMediaGroupId(), text);
                    if (!updated) {
                        if (message.getMediaGroupId() != null) {
                            processMediaGroup(message.getMediaGroupId());
                        } else if (message.getPhoto() != null && !message.getPhoto().isEmpty()) {
                            processSingleMessage(channelId, message.getMessageId(), text, message.getPhoto());
                        }
                    }
                });
                return;
            }

            if (message.getMediaGroupId() != null) {
                return;
            }
        }

        if (message.getMediaGroupId() != null) {
            storeMediaGroupItem(message);
            return;
        }

        if (!TextParser.containsKeyword(text, config.priceKeyword) && !TextParser.startsWithSaleHeader(text)) {
            db.markPostStatus(channelId, message.getMessageId(), "IGNORED");
            log.info("Message {} ignored (missing price keyword {})", message.getMessageId(), config.priceKeyword);
            return;
        }

        if (message.getPhoto() == null || message.getPhoto().isEmpty()) {
            db.markPostStatus(channelId, message.getMessageId(), "IGNORED");
            log.info("Message {} ignored (no photos)", message.getMessageId());
            return;
        }

        List<PhotoSize> photos = message.getPhoto();
        log.info("Message {} processing single photo set, photos={}", message.getMessageId(), photos.size());
        workers.submit(() -> processSingleMessage(channelId, message.getMessageId(), text, photos));
    }

    private void storeMediaGroupItem(Message message) {
        String channelId = String.valueOf(message.getChatId());
        String mediaGroupId = message.getMediaGroupId();
        List<PhotoSize> photos = message.getPhoto();
        if (photos == null || photos.isEmpty()) {
            return;
        }
        PhotoSize best = selectBestPhoto(photos);
        db.insertMediaItem(mediaGroupId, message.getMessageId(), best.getFileId(), best.getFileUniqueId(), best.getFileSize(), best.getWidth(), best.getHeight());
        db.upsertMediaGroup(mediaGroupId, channelId);
        log.info("Media group item stored: group={}, message={}, fileId={}", mediaGroupId, message.getMessageId(), best.getFileId());
    }

    private void processReadyMediaGroups() {
        long threshold = Instant.now().minusSeconds(config.mediaGroupFinalizeSeconds).getEpochSecond();
        for (String groupId : db.findReadyMediaGroups(threshold)) {
            if (db.markMediaGroupProcessing(groupId)) {
                log.info("Media group ready: {}", groupId);
                workers.submit(() -> processMediaGroup(groupId));
            }
        }
    }

    private void processMediaGroup(String mediaGroupId) {
        String channelId = db.findChannelIdForMediaGroup(mediaGroupId);
        if (channelId == null) return;
        Long existing = db.findProductIdForMediaGroup(mediaGroupId);
        if (existing != null) {
            db.markMediaGroupProcessed(mediaGroupId);
            log.info("Media group {} already processed as product {}, skipping", mediaGroupId, existing);
            return;
        }

        String text = db.findTextForMediaGroup(mediaGroupId);
        if (!TextParser.containsKeyword(text, config.priceKeyword) && !TextParser.startsWithSaleHeader(text)) {
            db.markMediaGroupProcessed(mediaGroupId);
            log.info("Media group {} ignored (missing price keyword {})", mediaGroupId, config.priceKeyword);
            return;
        }

        List<Database.MediaItem> items = db.listMediaItems(mediaGroupId);
        Long linkMessageId = items.isEmpty() ? null : items.get(0).messageId;
        String telegramLink = buildTelegramLink(channelId, linkMessageId);
        List<byte[]> images = new ArrayList<>();
        for (Database.MediaItem item : items) {
            try {
                images.add(downloadFileBytes(item.fileId));
            } catch (Exception e) {
                log.warn("Failed to download image for media group {} fileId={}", mediaGroupId, item.fileId, e);
            }
        }
        if (images.isEmpty()) {
            db.markMediaGroupProcessed(mediaGroupId);
            log.info("Media group {} has no images after download", mediaGroupId);
            return;
        }

        try {
            long productId = processListing(channelId, text, images, telegramLink);
            db.setProductIdForMediaGroup(channelId, mediaGroupId, productId);
            db.markMediaGroupProcessed(mediaGroupId);
            log.info("Media group {} processed into product {}", mediaGroupId, productId);
        } catch (Exception e) {
            log.error("Failed to process media group {}", mediaGroupId, e);
            db.markMediaGroupUnprocessed(mediaGroupId);
        }
    }

    private void processSingleMessage(String channelId, long messageId, String text, List<PhotoSize> photos) {
        try {
            PhotoSize best = selectBestPhoto(photos);
            byte[] image = downloadFileBytes(best.getFileId());
            String telegramLink = buildTelegramLink(channelId, messageId);
            long productId = processListing(channelId, text, List.of(image), telegramLink);
            db.setProductIdForMessage(channelId, messageId, productId);
            log.info("Message {} processed into product {}", messageId, productId);
        } catch (Exception e) {
            log.error("Failed to process message {}", messageId, e);
        }
    }

    private long processListing(String channelId, String text, List<byte[]> images, String telegramLink) throws IOException {
        boolean saleHeader = TextParser.startsWithSaleHeader(text);
        CategorySelection explicit = TextParser.detectExplicitCategories(text);
        if (saleHeader) {
            ensureSaleCategory(explicit);
        }
        String explicitHint = explicit.entries.isEmpty() ? "" : explicit.entries.get(0).section;

        Classification ai;
        boolean needsAi = explicit.entries.isEmpty() || hasOnlySaleCategory(explicit);
        ai = needsAi ? classifyWithFallback(text, images, explicitHint) : new Classification();
        CategorySelection resolved = resolveCategories(explicit, ai);

        String title = TextParser.extractTitle(text);
        if (title.isBlank()) title = ai.title;
        if (title == null || title.isBlank()) title = "Telegram Listing";

        String size = TextParser.extractSize(text);
        if (size.isBlank()) size = ai.size;

        String priceEur = TextParser.extractEur(text);
        if (priceEur.isBlank()) priceEur = ai.priceEur;

        String priceRsd = TextParser.extractRsd(text);
        if (priceRsd.isBlank()) priceRsd = ai.priceRsd;
        if ((priceEur == null || priceEur.isBlank()) && (priceRsd == null || priceRsd.isBlank())) {
            throw new IOException("Missing price (EUR and RSD are empty)");
        }

        TextParser.DiscountInfo discount = TextParser.extractDiscount(text);
        PriceSelection priceSelection = selectPrice(priceEur, priceRsd, discount);
        String bodyHtml = buildDescription(text, ai.description, priceSelection, discount, telegramLink);

        ShopifyClient.ProductPayload payload = new ShopifyClient.ProductPayload();
        payload.title = title;
        payload.bodyHtml = bodyHtml;
        payload.priceEur = priceSelection.price;
        payload.size = size;
        payload.tags = buildTags(resolved, ai);
        payload.productType = selectProductType(resolved);
        payload.images = images;

        long productId = shopify.createProduct(payload);
        if (telegramLink != null && !telegramLink.isBlank()) {
            try {
                shopify.setProductMetafield(productId,
                        config.telegramLinkMetafieldNamespace,
                        config.telegramLinkMetafieldKey,
                        telegramLink,
                        "link");
            } catch (Exception e) {
                log.warn("Failed to set telegram link metafield for product {}", productId, e);
            }
        }
        if (config.shopifyPublishAll) {
            try {
                shopify.publishProductToAll(productId);
                log.info("Product {} published to all channels", productId);
            } catch (Exception e) {
                log.warn("Failed to publish product {}", productId, e);
            }
        }

        for (CategorySelection.Entry entry : resolved.entries) {
            for (String titleKey : collections.buildCollectionTitles(entry.section, entry.subcategory)) {
                Long id = collections.getCollectionId(titleKey);
                if (id != null) {
                    try {
                        shopify.addProductToCollection(productId, id);
                        log.info("Product {} added to collection {}", productId, titleKey);
                    } catch (Exception e) {
                        log.warn("Failed to add product {} to collection {} (id={})", productId, titleKey, id, e);
                    }
                }
            }
        }

        log.info("Product created id={}, title='{}', price={}, priceSource={}, categories={}", productId, payload.title, payload.priceEur, priceSelection.source, resolved.entries.size());
        return productId;
    }

    private CategorySelection resolveCategories(CategorySelection explicit, Classification ai) {
        if (explicit != null && !explicit.entries.isEmpty()) {
            explicit.entries.forEach(e -> e.subcategory = null);
        }
        boolean onlySale = hasOnlySaleCategory(explicit);
        if (explicit != null && !explicit.isEmpty() && !onlySale) {
            explicit.entries.forEach(e -> e.subcategory = null);
            return explicit;
        }

        CategorySelection fallback = new CategorySelection();
        if (ai != null) {
            fallback.entries.addAll(ai.categories);
        }
        if (fallback.entries.isEmpty()) {
            fallback.add("Muško", null);
        }
        if (explicit != null && containsSaleCategory(explicit)) {
            ensureSaleCategory(fallback);
        }
        for (CategorySelection.Entry entry : fallback.entries) {
            entry.subcategory = null;
            if (!"Muško".equals(entry.section) && !"Žensko".equals(entry.section) && !"Sniženje".equals(entry.section)) {
                entry.section = "Muško";
            }
        }
        return fallback;
    }

    private String selectProductType(CategorySelection selection) {
        if (selection.entries.isEmpty()) return "";
        CategorySelection.Entry entry = selection.entries.get(0);
        return entry.subcategory != null ? entry.subcategory : entry.section;
    }

    private List<String> buildTags(CategorySelection selection, Classification ai) {
        List<String> tags = new ArrayList<>();
        for (CategorySelection.Entry entry : selection.entries) {
            tags.add("section:" + entry.section);
            if (entry.subcategory != null && !entry.subcategory.isBlank()) {
                tags.add("subcategory:" + entry.subcategory);
            }
        }
        if (ai != null && ai.tags != null) {
            for (String t : ai.tags) {
                if (t != null && !t.isBlank()) {
                    tags.add(t.trim());
                }
            }
        }
        return tags;
    }

    private String buildDescription(String originalText, String aiDescription, PriceSelection priceSelection, TextParser.DiscountInfo discount, String telegramLink) {
        String base = (originalText != null && !originalText.isBlank()) ? originalText : aiDescription;
        if (base == null) base = "";
        base = TextParser.normalizeNewlines(base);
        base = TextParser.removeLinesStartingWith(base, java.util.List.of("ako"));
        base = TextParser.normalizeSaleLines(base);
        return base.replace("\n", "<br>");
    }

    private String buildTelegramLink(String channelId, Long messageId) {
        if (messageId == null || channelId == null || channelId.isBlank()) return null;
        String username = config.telegramChannelUsername;
        if (username != null && !username.isBlank()) {
            String clean = username.startsWith("@") ? username.substring(1) : username;
            return "https://t.me/" + clean + "/" + messageId;
        }
        String normalized = channelId.trim();
        if (normalized.startsWith("-100")) {
            normalized = normalized.substring(4);
        } else if (normalized.startsWith("-")) {
            normalized = normalized.substring(1);
        }
        if (normalized.isBlank()) return null;
        return "https://t.me/c/" + normalized + "/" + messageId;
    }

    private boolean hasOnlySaleCategory(CategorySelection selection) {
        if (selection == null || selection.entries.isEmpty()) return false;
        boolean hasSale = false;
        boolean hasOther = false;
        for (CategorySelection.Entry entry : selection.entries) {
            if ("Sniženje".equals(entry.section)) {
                hasSale = true;
            } else {
                hasOther = true;
            }
        }
        return hasSale && !hasOther;
    }

    private boolean containsSaleCategory(CategorySelection selection) {
        if (selection == null || selection.entries.isEmpty()) return false;
        for (CategorySelection.Entry entry : selection.entries) {
            if ("Sniženje".equals(entry.section)) return true;
        }
        return false;
    }

    private void ensureSaleCategory(CategorySelection selection) {
        if (selection == null) return;
        if (!containsSaleCategory(selection)) {
            selection.add("Sniženje", null);
        }
    }

    private String highlightLineWithKeyword(String text, String keyword) {
        if (text == null || text.isBlank() || keyword == null || keyword.isBlank()) {
            return text;
        }
        String[] lines = text.split("\n");
        String keywordLower = keyword.toLowerCase(Locale.ROOT);
        String spanStart = "<span style=\"color:red;display:inline\">";
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            String current = line;
            String lowerLine = line.toLowerCase(Locale.ROOT);
            if (lowerLine.contains(keywordLower) && !lowerLine.contains("<span") && !lowerLine.contains("</span>")) {
                current = spanStart + line + "</span>";
            }
            if (sb.length() > 0) sb.append("\n");
            sb.append(current);
        }
        return sb.toString();
    }

    private String highlightSnizenjeWord(String text) {
        if (text == null || text.isBlank()) return text;
        Pattern pattern = Pattern.compile("(?iu)\\b(sniženje|snizenje|снижение)\\b");
        Matcher matcher = pattern.matcher(text);
        StringBuffer sb = new StringBuffer();
        String spanStart = "<span style=\"color:red;display:inline\">";
        while (matcher.find()) {
            String match = matcher.group(1);
            String replacement = spanStart + match + "</span>";
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String highlightNumbers(String text) {
        if (text == null || text.isBlank()) return text;
        Pattern pattern = Pattern.compile("(?u)(\\d[\\d.,]*)");
        Matcher matcher = pattern.matcher(text);
        StringBuffer sb = new StringBuffer();
        String spanStart = "<span style=\"color:red;display:inline\">";
        while (matcher.find()) {
            String match = matcher.group(1);
            String replacement = spanStart + match + "</span>";
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private PriceSelection selectPrice(String priceEur, String priceRsd, TextParser.DiscountInfo discount) throws IOException {
        if (discount != null && discount.finalPrice != null && !discount.finalPrice.isBlank()) {
            if ("RSD".equalsIgnoreCase(shopCurrency)) {
                priceRsd = discount.finalPrice;
            } else if ("EUR".equalsIgnoreCase(shopCurrency)) {
                priceEur = discount.finalPrice;
            } else {
                priceRsd = discount.finalPrice;
            }
        }
        String source = config.priceSource == null ? "AUTO" : config.priceSource.trim().toUpperCase(Locale.ROOT);
        String resolved = priceEur;
        String resolvedSource = "EUR";

        if ("RSD".equals(source)) {
            resolved = (priceRsd == null || priceRsd.isBlank()) ? priceEur : priceRsd;
            resolvedSource = "RSD";
        } else if ("EUR".equals(source)) {
            resolved = priceEur;
            resolvedSource = "EUR";
        } else {
            if (discount != null && discount.finalPrice != null && !discount.finalPrice.isBlank()) {
                resolved = discount.finalPrice;
                resolvedSource = "AUTO";
            }
            if ("RSD".equalsIgnoreCase(shopCurrency) && priceRsd != null && !priceRsd.isBlank()) {
                resolved = priceRsd;
                resolvedSource = "RSD";
            } else {
                resolved = priceEur;
                resolvedSource = "EUR";
            }
        }

        if (resolved == null || resolved.isBlank()) {
            throw new IOException("Missing price after selection. eur=" + priceEur + " rsd=" + priceRsd + " source=" + source);
        }

        return new PriceSelection(resolved, resolvedSource, priceEur, priceRsd);
    }

    private static class PriceSelection {
        public final String price;
        public final String source;
        public final String eur;
        public final String rsd;

        public PriceSelection(String price, String source, String eur, String rsd) {
            this.price = price;
            this.source = source;
            this.eur = eur;
            this.rsd = rsd;
        }
    }

    private void handleSold(String channelId, long messageId, String mediaGroupId) {
        Long productId = db.findProductId(channelId, messageId);
        if (productId == null) return;
        try {
            shopify.deleteProduct(productId);
            db.markProductStatus(productId, "SOLD");
            log.info("Product {} deleted in Shopify (sold)", productId);
        } catch (Exception e) {
            log.warn("Failed to delete product {} (sold)", productId, e);
        }
        deleteTelegramByReference(channelId, messageId, mediaGroupId);
    }

    private boolean updateExistingProduct(String channelId, long messageId, String mediaGroupId, String text) {
        Long productId = db.findProductId(channelId, messageId);
        if (productId == null) return false;
        try {
            if (!shopify.productExists(productId)) {
                if (mediaGroupId != null && !mediaGroupId.isBlank()) {
                    db.clearProductForMediaGroup(channelId, mediaGroupId);
                } else {
                    db.clearProductForMessage(channelId, messageId);
                }
                log.info("Product {} missing in Shopify, clearing reference for message {}", productId, messageId);
                return false;
            }
            String title = TextParser.extractTitle(text);
            String size = TextParser.extractSize(text);

            String priceEur = TextParser.extractEur(text);
            String priceRsd = TextParser.extractRsd(text);
            TextParser.DiscountInfo discount = TextParser.extractDiscount(text);
            PriceSelection priceSelection = selectPrice(priceEur, priceRsd, discount);

            String telegramLink = buildTelegramLink(channelId, messageId);
            String bodyHtml = buildDescription(text, "", priceSelection, discount, telegramLink);
            ShopifyProductSnapshot snap = shopify.getProductSnapshot(productId);
            shopify.updateProduct(productId, snap.variantId, title, bodyHtml, priceSelection.price, size);
            log.info("Product {} updated from edited message {}", productId, messageId);
            if (telegramLink != null && !telegramLink.isBlank()) {
                try {
                shopify.setProductMetafield(productId,
                        config.telegramLinkMetafieldNamespace,
                        config.telegramLinkMetafieldKey,
                        telegramLink,
                        "link");
                } catch (Exception e) {
                    log.warn("Failed to set telegram link metafield for product {}", productId, e);
                }
            }

            try {
                boolean saleHeader = TextParser.startsWithSaleHeader(text);
                CategorySelection explicit = TextParser.detectExplicitCategories(text);
                if (saleHeader) {
                    ensureSaleCategory(explicit);
                }
                String explicitHint = explicit.entries.isEmpty() ? "" : explicit.entries.get(0).section;
                boolean needsAi = explicit.entries.isEmpty() || hasOnlySaleCategory(explicit);
                Classification ai = needsAi ? classifyWithFallback(text, List.of(), explicitHint) : new Classification();
                CategorySelection resolved = resolveCategories(explicit, ai);

                Set<String> desiredSections = new LinkedHashSet<>();
                for (CategorySelection.Entry entry : resolved.entries) {
                    for (String titleKey : collections.buildCollectionTitles(entry.section, entry.subcategory)) {
                        desiredSections.add(titleKey);
                    }
                }

                for (String section : collections.listSectionTitles()) {
                    Long id = collections.getCollectionId(section);
                    if (id == null) continue;
                    if (desiredSections.contains(section)) {
                        try {
                            shopify.addProductToCollection(productId, id);
                        } catch (Exception e) {
                            log.warn("Failed to add product {} to collection {} (edit)", productId, section, e);
                        }
                    } else {
                        try {
                            shopify.removeProductFromCollection(id, productId);
                        } catch (Exception e) {
                            log.warn("Failed to remove product {} from collection {} (edit)", productId, section, e);
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to recalc collections for edited message {}", messageId, e);
            }
            return true;
        } catch (Exception e) {
            log.warn("Failed to update product from edited message {}", messageId, e);
            return false;
        }
    }

    private Classification classifyWithFallback(String text, List<byte[]> images, String explicitHint) throws IOException {
        int attempts = Math.max(1, config.kieRetryAttempts);
        long delayMs = Math.max(0, config.kieRetryDelayMs);
        Classification last = null;
        IOException lastIo = null;

        for (int attempt = 1; attempt <= attempts; attempt++) {
            boolean usePrimary = (attempt % 2 == 1);
            String modelName = usePrimary ? config.kieModel : config.kieFallbackModel;
            try {
                Classification result = usePrimary
                        ? kie.classify(text, images, explicitHint)
                        : kie.classify(text, images, explicitHint, config.kieFallbackModel, config.kieFallbackEndpointOverride);
                last = result;
                if (result != null && result.categories != null && !result.categories.isEmpty()) {
                    log.info("AI model {} success after {} attempt(s), categories={}", modelName, attempt, result.categories.size());
                    return result;
                }
                log.warn("AI model {} returned empty categories (attempt {}/{})", modelName, attempt, attempts);
            } catch (IOException e) {
                lastIo = e;
                log.warn("AI model {} failed (attempt {}/{}): {}", modelName, attempt, attempts, e.getMessage());
            } catch (Exception e) {
                log.warn("AI model {} unexpected failure (attempt {}/{}): {}", modelName, attempt, attempts, e.getMessage());
            }
            if (attempt < attempts && delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (last != null) return last;
        if (lastIo != null) throw lastIo;
        return new Classification();
    }

    private void deleteTelegramByReference(String channelId, long messageId, String mediaGroupId) {
        List<Long> ids = new ArrayList<>();
        if (mediaGroupId != null && !mediaGroupId.isBlank()) {
            ids.addAll(db.listMessageIdsForMediaGroup(mediaGroupId));
        } else {
            ids.add(messageId);
        }
        deleteTelegramMessages(channelId, ids);
    }

    private void deleteTelegramMessages(String channelId, List<Long> messageIds) {
        for (Long id : messageIds) {
            if (id == null) continue;
            try {
                DeleteMessage del = new DeleteMessage();
                del.setChatId(channelId);
                del.setMessageId(id.intValue());
                execute(del);
                log.info("Telegram message deleted: channel={}, message={}", channelId, id);
            } catch (Exception e) {
                log.warn("Failed to delete Telegram message: channel={}, message={}", channelId, id, e);
            }
        }
    }

    private PhotoSize selectBestPhoto(List<PhotoSize> photos) {
        return photos.stream()
                .sorted(Comparator.comparingInt(p -> p.getFileSize() == null ? 0 : p.getFileSize()))
                .filter(p -> p.getFileSize() == null || p.getFileSize() <= config.maxImageBytes)
                .reduce((first, second) -> second)
                .orElseGet(() -> photos.get(photos.size() - 1));
    }

    private byte[] downloadFileBytes(String fileId) throws IOException {
        try {
            org.telegram.telegrambots.meta.api.objects.File file = execute(new GetFile(fileId));
            Path temp = Files.createTempFile("tg", ".bin");
            java.io.File downloaded = downloadFile(file, temp.toFile());
            byte[] data = Files.readAllBytes(downloaded.toPath());
            Files.deleteIfExists(downloaded.toPath());
            return data;
        } catch (Exception e) {
            throw new IOException("Failed to download Telegram file", e);
        }
    }

    private String extractText(Message message) {
        if (message.getText() != null) return message.getText();
        if (message.getCaption() != null) return message.getCaption();
        return "";
    }

    private void syncDeletedProducts() {
        List<PostRef> posts = db.listProcessedPosts();
        if (posts.isEmpty()) return;

        java.util.Map<Long, PostRef> byProduct = new java.util.HashMap<>();
        for (PostRef ref : posts) {
            byProduct.putIfAbsent(ref.productId, ref);
        }

        for (PostRef ref : byProduct.values()) {
            try {
                if (!shopify.productExists(ref.productId)) {
                    deleteTelegramByReference(ref.channelId, ref.messageId, ref.mediaGroupId);
                    db.markProductStatus(ref.productId, "MISSING");
                    log.info("Product missing in Shopify, deleted Telegram message(s). productId={}", ref.productId);
                }
            } catch (Exception e) {
                log.warn("Failed to sync product {} existence", ref.productId, e);
            }
        }
    }

    @Override
    public String getBotUsername() {
        return "ShopifyBridgeBot";
    }
}
