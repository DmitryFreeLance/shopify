package com.shopifybot.telegram;

import com.shopifybot.Config;
import com.shopifybot.ai.CategorySelection;
import com.shopifybot.ai.Classification;
import com.shopifybot.ai.KieAiClient;
import com.shopifybot.db.Database;
import com.shopifybot.db.Database.PostRef;
import com.shopifybot.shopify.ShopifyClient;
import com.shopifybot.shopify.ShopifyCollections;
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
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ShopifyBot extends TelegramLongPollingBot {
    private static final Logger log = LoggerFactory.getLogger(ShopifyBot.class);
    private final Config config;
    private final Database db;
    private final ShopifyClient shopify;
    private final ShopifyCollections collections;
    private final KieAiClient kie;

    private final ExecutorService workers = Executors.newFixedThreadPool(2);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public ShopifyBot(Config config, Database db, OkHttpClient http) {
        super(config.telegramBotToken);
        this.config = config;
        this.db = db;
        this.shopify = new ShopifyClient(http, config.shopifyShopDomain, config.shopifyApiVersion, config.shopifyAdminToken);
        this.collections = new ShopifyCollections(shopify, db);
        this.kie = new KieAiClient(http, config.kieApiKey, config.kieModel, config.kieBaseUrl, config.kieEndpointOverride);

        scheduler.scheduleWithFixedDelay(this::processReadyMediaGroups, 15, 15, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::syncDeletedProducts, config.productSyncSeconds, config.productSyncSeconds, TimeUnit.SECONDS);
    }

    public void initialize() {
        try {
            collections.ensureCollections();
            log.info("Collections ensured");
        } catch (IOException e) {
            throw new RuntimeException("Failed to ensure Shopify collections", e);
        }
    }

    @Override
    public void onUpdateReceived(Update update) {
        log.info("Update received: channel_post={}, edited_channel_post={}", update.hasChannelPost(), update.hasEditedChannelPost());
        if (update.hasChannelPost()) {
            handleMessage(update.getChannelPost(), false);
        }
        if (update.hasEditedChannelPost()) {
            handleMessage(update.getEditedChannelPost(), true);
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
        }

        if (!TextParser.containsKeyword(text, config.priceKeyword)) {
            db.markPostStatus(channelId, message.getMessageId(), "IGNORED");
            log.info("Message {} ignored (missing price keyword {})", message.getMessageId(), config.priceKeyword);
            return;
        }

        if (message.getMediaGroupId() != null) {
            storeMediaGroupItem(message);
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
            log.info("Media group ready: {}", groupId);
            workers.submit(() -> processMediaGroup(groupId));
        }
    }

    private void processMediaGroup(String mediaGroupId) {
        String channelId = db.findChannelIdForMediaGroup(mediaGroupId);
        if (channelId == null) return;

        String text = db.findTextForMediaGroup(mediaGroupId);
        if (!TextParser.containsKeyword(text, config.priceKeyword)) {
            db.markMediaGroupProcessed(mediaGroupId);
            log.info("Media group {} ignored (missing price keyword {})", mediaGroupId, config.priceKeyword);
            return;
        }

        List<Database.MediaItem> items = db.listMediaItems(mediaGroupId);
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
            long productId = processListing(channelId, text, images);
            db.setProductIdForMediaGroup(channelId, mediaGroupId, productId);
            db.markMediaGroupProcessed(mediaGroupId);
            log.info("Media group {} processed into product {}", mediaGroupId, productId);
        } catch (Exception e) {
            log.error("Failed to process media group {}", mediaGroupId, e);
        }
    }

    private void processSingleMessage(String channelId, long messageId, String text, List<PhotoSize> photos) {
        try {
            PhotoSize best = selectBestPhoto(photos);
            byte[] image = downloadFileBytes(best.getFileId());
            long productId = processListing(channelId, text, List.of(image));
            db.setProductIdForMessage(channelId, messageId, productId);
            log.info("Message {} processed into product {}", messageId, productId);
        } catch (Exception e) {
            log.error("Failed to process message {}", messageId, e);
        }
    }

    private long processListing(String channelId, String text, List<byte[]> images) throws IOException {
        CategorySelection explicit = TextParser.detectExplicitCategories(text);
        String explicitHint = explicit.entries.isEmpty() ? "" : explicit.entries.get(0).section;

        Classification ai = classifyWithFallback(text, images, explicitHint);
        CategorySelection resolved = resolveCategories(explicit, ai);

        String title = TextParser.extractTitle(text);
        if (title.isBlank()) title = ai.title;
        if (title == null || title.isBlank()) title = "Telegram Listing";

        String size = TextParser.extractSize(text);
        if (size.isBlank()) size = ai.size;

        String priceEur = TextParser.extractEur(text);
        if (priceEur.isBlank()) priceEur = ai.priceEur;
        if (priceEur == null || priceEur.isBlank()) {
            throw new IOException("Missing EUR price");
        }

        String priceRsd = TextParser.extractRsd(text);
        if (priceRsd.isBlank()) priceRsd = ai.priceRsd;

        String bodyHtml = buildDescription(text, ai.description, priceRsd);

        ShopifyClient.ProductPayload payload = new ShopifyClient.ProductPayload();
        payload.title = title;
        payload.bodyHtml = bodyHtml;
        payload.priceEur = priceEur;
        payload.size = size;
        payload.tags = buildTags(resolved, ai);
        payload.productType = selectProductType(resolved);
        payload.images = images;

        long productId = shopify.createProduct(payload);
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
                    shopify.addProductToCollection(productId, id);
                    log.info("Product {} added to collection {}", productId, titleKey);
                }
            }
        }

        log.info("Product created id={}, title='{}', priceEUR={}, categories={}", productId, payload.title, payload.priceEur, resolved.entries.size());
        return productId;
    }

    private CategorySelection resolveCategories(CategorySelection explicit, Classification ai) {
        if (!explicit.isEmpty()) {
            if (!explicit.entries.isEmpty()) {
                String fillSub = null;
                if (ai != null && !ai.categories.isEmpty()) {
                    fillSub = ai.categories.get(0).subcategory;
                }
                for (CategorySelection.Entry entry : explicit.entries) {
                    if (entry.subcategory == null && fillSub != null) {
                        entry.subcategory = fillSub;
                    }
                }
            }
            return explicit;
        }

        CategorySelection fallback = new CategorySelection();
        if (ai != null) {
            fallback.entries.addAll(ai.categories);
        }
        if (fallback.entries.isEmpty()) {
            fallback.add("Muško", null);
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

    private String buildDescription(String originalText, String aiDescription, String priceRsd) {
        String base = (originalText != null && !originalText.isBlank()) ? originalText : aiDescription;
        if (base == null) base = "";
        if (priceRsd != null && !priceRsd.isBlank()) {
            String lower = base.toLowerCase(Locale.ROOT);
            if (!lower.contains(priceRsd)) {
                base = base + "\nЦена RSD: " + priceRsd;
            }
        }
        return base.replace("\n", "<br>");
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

    private Classification classifyWithFallback(String text, List<byte[]> images, String explicitHint) throws IOException {
        try {
            Classification primary = kie.classify(text, images, explicitHint);
            if (primary != null && primary.categories != null && !primary.categories.isEmpty()) {
                log.info("AI primary model success, categories={}", primary.categories.size());
                return primary;
            }
            log.info("AI primary model returned empty categories, using fallback");
        } catch (Exception ignored) {
            log.info("AI primary model failed, using fallback");
        }
        Classification fallback = kie.classify(text, images, explicitHint, config.kieFallbackModel, config.kieFallbackEndpointOverride);
        log.info("AI fallback model used, categories={}", fallback.categories == null ? 0 : fallback.categories.size());
        return fallback;
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
