package com.shopifybot.telegram;

import com.shopifybot.Config;
import com.shopifybot.ai.CategorySelection;
import com.shopifybot.ai.Classification;
import com.shopifybot.ai.KieAiClient;
import com.shopifybot.db.Database;
import com.shopifybot.db.Database.ProductCard;
import com.shopifybot.db.Database.PostRef;
import com.shopifybot.db.Database.UserRecord;
import com.shopifybot.shopify.ShopifyClient;
import com.shopifybot.shopify.ShopifyCollections;
import com.shopifybot.shopify.ShopifyProductSnapshot;
import com.shopifybot.shopify.RateLimitException;
import com.shopifybot.shopify.TokenProvider;
import com.shopifybot.util.TextParser;
import okhttp3.OkHttpClient;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.AnswerCallbackQuery;
import org.telegram.telegrambots.meta.api.methods.send.SendMediaGroup;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.DeleteMessage;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageCaption;
import org.telegram.telegrambots.meta.api.methods.GetFile;
import org.telegram.telegrambots.meta.api.objects.CallbackQuery;
import org.telegram.telegrambots.meta.api.objects.InputFile;
import org.telegram.telegrambots.meta.api.objects.Message;
import org.telegram.telegrambots.meta.api.objects.PhotoSize;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.api.objects.User;
import org.telegram.telegrambots.meta.api.objects.media.InputMedia;
import org.telegram.telegrambots.meta.api.objects.media.InputMediaPhoto;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShopifyBot extends TelegramLongPollingBot {
    private static final Logger log = LoggerFactory.getLogger(ShopifyBot.class);
    private static final String ORDER_CONTACT_FOOTER = "Ako želite da naručite neku stvar, pošaljite fotografiju ove stvari @alinka809 ili @hlestovdmitry";
    private static final String CB_MENU = "MENU";
    private static final String CB_NOOP = "NOOP";
    private static final String CB_ADD_PRODUCT = "OPEN:ADD_PRODUCT";
    private static final String CB_PRODUCTS = "OPEN:PRODUCTS";
    private static final String CB_RESERVE = "OPEN:RESERVE";
    private static final String CB_UNRESERVE = "OPEN:UNRESERVE";
    private static final String CB_SOLD = "OPEN:SOLD";
    private static final String CB_USERS = "OPEN:USERS";
    private static final String CB_ADD_ADMIN = "OPEN:ADD_ADMIN";
    private static final String CB_DISCOUNTS = "OPEN:DISCOUNTS";
    private static final String CB_DISCOUNTS_DISABLE = "DISCOUNT:DISABLE";
    private static final String CB_DISCOUNTS_ENABLE = "DISCOUNT:ENABLE";
    private static final String CB_MANUAL_DISCOUNT = "OPEN:MANUAL_DISCOUNT";
    private static final String CB_DONE_PHOTOS = "FLOW:DONE_PHOTOS";
    private static final String CB_BACK_TO_PHOTOS = "FLOW:BACK_TO_PHOTOS";
    private static final String CB_CANCEL_FLOW = "FLOW:CANCEL";
    private static final String META_DISCOUNT_ENABLED = "discount:enabled";
    private static final String META_DISCOUNT_RESET_START = "discount:reset_start_date";

    private final Config config;
    private final Database db;
    private final ShopifyClient shopify;
    private final ShopifyCollections collections;
    private final KieAiClient kie;
    private String shopCurrency;
    private ZoneId discountZone;

    private final ExecutorService workers = Executors.newFixedThreadPool(2);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Map<Long, AdminSession> sessions = new ConcurrentHashMap<>();

    public ShopifyBot(Config config, Database db, OkHttpClient http, TokenProvider tokenProvider) {
        super(config.telegramBotToken);
        this.config = config;
        this.db = db;
        this.shopify = new ShopifyClient(http, config.shopifyShopDomain, config.shopifyApiVersion, tokenProvider);
        this.collections = new ShopifyCollections(shopify, db);
        this.kie = new KieAiClient(http, config.kieApiKey, config.kieModel, config.kieBaseUrl, config.kieEndpointOverride);

        scheduler.scheduleWithFixedDelay(this::processReadyMediaGroups, 15, 15, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::syncDeletedProductsSafe, config.productSyncSeconds, config.productSyncSeconds, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::syncDiscountsSafe, 60, Math.max(300, config.discountSyncSeconds), TimeUnit.SECONDS);
    }

    public void initialize() {
        try {
            collections.ensureCollections();
            discountZone = ZoneId.of(config.discountTimezone);
            shopCurrency = shopify.getShopCurrency();
            if (shopCurrency == null || shopCurrency.isBlank()) {
                shopCurrency = "UNKNOWN";
            }
            for (Long adminId : config.adminUserIds) {
                db.addAdmin(adminId, "", null);
            }
            log.info("Shop currency detected: {}", shopCurrency);
            log.info("Admins loaded: {}", config.adminUserIds.size());
            log.info("Collections ensured");
        } catch (IOException e) {
            throw new RuntimeException("Failed to ensure Shopify collections", e);
        } catch (Exception e) {
            throw new RuntimeException("Initialization failed", e);
        }
    }

    @Override
    public void onUpdateReceived(Update update) {
        if (update.hasCallbackQuery()) {
            handleCallback(update.getCallbackQuery());
            return;
        }
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
            handleIncomingMessage(update.getChannelPost(), false);
        }
        if (update.hasEditedChannelPost()) {
            handleIncomingMessage(update.getEditedChannelPost(), true);
        }
        if (update.hasMessage()) {
            handleIncomingMessage(update.getMessage(), false);
        }
        if (update.hasEditedMessage()) {
            handleIncomingMessage(update.getEditedMessage(), true);
        }
    }

    private void handleIncomingMessage(Message message, boolean isEdit) {
        User user = message.getFrom();
        if (user != null) {
            boolean isAdmin = db.isAdmin(user.getId());
            db.upsertUser(
                    user.getId(),
                    user.getUserName(),
                    user.getFirstName(),
                    user.getLastName(),
                    isAdmin
            );
        }

        if (message.getChat() != null && "private".equalsIgnoreCase(message.getChat().getType())) {
            if (isEdit) {
                return;
            }
            handlePrivateAdminMessage(message);
            return;
        }

        handleMessage(message, isEdit);
    }

    private void handleCallback(CallbackQuery callback) {
        if (callback == null || callback.getMessage() == null || callback.getData() == null) {
            return;
        }
        User user = callback.getFrom();
        if (user == null || !db.isAdmin(user.getId())) {
            answerCallback(callback, "Доступ только для админов");
            return;
        }
        long chatId = callback.getMessage().getChatId();
        AdminSession session = sessionFor(user.getId());

        String data = callback.getData();
        if (CB_NOOP.equals(data)) {
            answerCallback(callback, "");
            return;
        }
        if (CB_MENU.equals(data)) {
            resetSession(session);
            sendWelcomeMenu(chatId);
            answerCallback(callback, "");
            return;
        }
        if (CB_CANCEL_FLOW.equals(data)) {
            resetSession(session);
            sendWelcomeMenu(chatId, "Операция отменена.");
            answerCallback(callback, "Операция отменена");
            return;
        }
        if (CB_ADD_PRODUCT.equals(data)) {
            resetSession(session);
            session.state = AdminState.ADD_PRODUCT_PHOTOS;
            sendPhotoUploadPrompt(chatId, session.pendingPhotoFileIds.size());
            answerCallback(callback, "");
            return;
        }
        if (CB_PRODUCTS.equals(data)) {
            resetSession(session);
            sendProductsPage(chatId, 0);
            answerCallback(callback, "");
            return;
        }
        if (CB_RESERVE.equals(data)) {
            resetSession(session);
            session.state = AdminState.RESERVE_SELECT;
            sendSelectableProductsPage(chatId, session, 0);
            answerCallback(callback, "");
            return;
        }
        if (CB_UNRESERVE.equals(data)) {
            resetSession(session);
            session.state = AdminState.UNRESERVE_SELECT;
            sendSelectableProductsPage(chatId, session, 0);
            answerCallback(callback, "");
            return;
        }
        if (CB_SOLD.equals(data)) {
            resetSession(session);
            session.state = AdminState.SOLD_SELECT;
            sendSelectableProductsPage(chatId, session, 0);
            answerCallback(callback, "");
            return;
        }
        if (CB_USERS.equals(data)) {
            resetSession(session);
            sendUsersPage(chatId, 0);
            answerCallback(callback, "");
            return;
        }
        if (CB_ADD_ADMIN.equals(data)) {
            resetSession(session);
            session.state = AdminState.ADD_ADMIN_ID;
            sendText(chatId,
                    "Введите Telegram ID пользователя, которого нужно сделать админом.",
                    inlineSingleColumn(
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            answerCallback(callback, "");
            return;
        }
        if (CB_DISCOUNTS.equals(data)) {
            resetSession(session);
            sendDiscountsDashboard(chatId);
            answerCallback(callback, "");
            return;
        }
        if (CB_DISCOUNTS_DISABLE.equals(data)) {
            db.setMeta(META_DISCOUNT_ENABLED, "false");
            sendDiscountsDashboard(chatId, "⏸ Прогрессивные скидки отключены.");
            answerCallback(callback, "Скидки отключены");
            return;
        }
        if (CB_DISCOUNTS_ENABLE.equals(data)) {
            db.setMeta(META_DISCOUNT_ENABLED, "true");
            String today = todayInDiscountZone().toString();
            db.setMeta(META_DISCOUNT_RESET_START, today);
            db.setMeta("discount:last_sync_date", "");
            sendDiscountsDashboard(chatId, "▶️ Прогрессивные скидки включены.\nДля новых товаров цикл начинается с дня 1 (0%).");
            answerCallback(callback, "Скидки включены");
            return;
        }
        if (CB_MANUAL_DISCOUNT.equals(data)) {
            resetSession(session);
            session.state = AdminState.MANUAL_DISCOUNT_SELECT;
            sendSelectableProductsPage(chatId, session, 0);
            answerCallback(callback, "");
            return;
        }
        if (CB_DONE_PHOTOS.equals(data)) {
            if (session.state != AdminState.ADD_PRODUCT_PHOTOS) {
                session.state = AdminState.ADD_PRODUCT_PHOTOS;
            }
            if (session.pendingPhotoFileIds.isEmpty()) {
                sendPhotoUploadPrompt(chatId, 0);
                answerCallback(callback, "Сначала добавьте хотя бы одно фото");
                return;
            }
            session.state = AdminState.ADD_PRODUCT_DESCRIPTION;
            sendText(chatId,
                    "Теперь отправьте описание товара.\n\nПример:\nAdidas\nVel - ženski S\nCena - 1500 RSD\nArtikal: 56789356",
                    inlineSingleColumn(
                            button("Назад", CB_BACK_TO_PHOTOS),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            answerCallback(callback, "");
            return;
        }
        if (CB_BACK_TO_PHOTOS.equals(data)) {
            session.state = AdminState.ADD_PRODUCT_PHOTOS;
            sendPhotoUploadPrompt(chatId, session.pendingPhotoFileIds.size());
            answerCallback(callback, "");
            return;
        }

        if (data.startsWith("PAGE:")) {
            String[] parts = data.split(":");
            if (parts.length != 3) {
                answerCallback(callback, "Некорректная команда");
                return;
            }
            int page;
            try {
                page = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                answerCallback(callback, "Некорректная страница");
                return;
            }
            switch (parts[1]) {
                case "PRODUCTS":
                    sendProductsPage(chatId, page);
                    break;
                case "USERS":
                    sendUsersPage(chatId, page);
                    break;
                case "RESERVE":
                    session.state = AdminState.RESERVE_SELECT;
                    sendSelectableProductsPage(chatId, session, page);
                    break;
                case "UNRESERVE":
                    session.state = AdminState.UNRESERVE_SELECT;
                    sendSelectableProductsPage(chatId, session, page);
                    break;
                case "SOLD":
                    session.state = AdminState.SOLD_SELECT;
                    sendSelectableProductsPage(chatId, session, page);
                    break;
                case "MANUAL":
                    session.state = AdminState.MANUAL_DISCOUNT_SELECT;
                    sendSelectableProductsPage(chatId, session, page);
                    break;
                default:
                    break;
            }
            answerCallback(callback, "");
            return;
        }
        answerCallback(callback, "");
    }

    private void handlePrivateAdminMessage(Message message) {
        User user = message.getFrom();
        if (user == null) return;
        long chatId = message.getChatId();
        boolean isAdmin = db.isAdmin(user.getId());
        db.upsertUser(user.getId(), user.getUserName(), user.getFirstName(), user.getLastName(), isAdmin);

        if (!isAdmin) {
            sendText(chatId, "⛔ Доступ только для администраторов.");
            return;
        }

        AdminSession session = sessionFor(user.getId());
        String text = extractText(message).trim();

        if ("/start".equalsIgnoreCase(text) || "/menu".equalsIgnoreCase(text)) {
            resetSession(session);
            sendWelcomeMenu(chatId);
            return;
        }
        if ("/add".equalsIgnoreCase(text)) {
            resetSession(session);
            session.state = AdminState.ADD_ADMIN_ID;
            sendText(chatId,
                    "Введите Telegram ID пользователя, которого нужно сделать админом.",
                    inlineSingleColumn(
                            button("👥 Список пользователей", CB_USERS),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }
        if ("/discounts".equalsIgnoreCase(text)) {
            resetSession(session);
            sendDiscountsDashboard(chatId);
            return;
        }
        if ("/cancel".equalsIgnoreCase(text)) {
            resetSession(session);
            sendWelcomeMenu(chatId);
            return;
        }

        if (session.state == AdminState.ADD_PRODUCT_PHOTOS) {
            if (message.getPhoto() != null && !message.getPhoto().isEmpty()) {
                if (session.pendingPhotoFileIds.size() >= 9) {
                    sendPhotoUploadPrompt(chatId, session.pendingPhotoFileIds.size());
                    return;
                }
                PhotoSize best = selectBestPhoto(message.getPhoto());
                session.pendingPhotoFileIds.add(best.getFileId());
                sendPhotoUploadPrompt(chatId, session.pendingPhotoFileIds.size());
            } else {
                sendPhotoUploadPrompt(chatId, session.pendingPhotoFileIds.size());
            }
            return;
        }

        if (session.state == AdminState.ADD_PRODUCT_DESCRIPTION) {
            if (text.isBlank()) {
                sendText(chatId,
                        "✍️ Описание пустое. Отправьте текст описания товара.",
                        inlineSingleColumn(
                                button("Назад", CB_BACK_TO_PHOTOS),
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
                return;
            }
            createProductFromAdminInput(chatId, session, text);
            return;
        }

        if (session.state == AdminState.ADD_ADMIN_ID) {
            long newAdminId;
            try {
                newAdminId = Long.parseLong(text);
            } catch (NumberFormatException e) {
                sendText(chatId,
                        "Введите числовой Telegram ID (например: 123456789).",
                        inlineSingleColumn(
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
                return;
            }
            db.addAdmin(newAdminId, "", user.getId());
            resetSession(session);
            sendWelcomeMenu(chatId, "✅ Админ добавлен: " + newAdminId);
            return;
        }

        if (session.state == AdminState.RESERVE_SELECT
                || session.state == AdminState.UNRESERVE_SELECT
                || session.state == AdminState.SOLD_SELECT
                || session.state == AdminState.MANUAL_DISCOUNT_SELECT) {
            int ordinal;
            try {
                ordinal = Integer.parseInt(text);
            } catch (NumberFormatException e) {
                sendText(chatId, "Введите номер позиции цифрой (например: 1).");
                return;
            }
            processSelectionByOrdinal(chatId, session, ordinal);
            return;
        }

        if (session.state == AdminState.MANUAL_DISCOUNT_INPUT) {
            if (session.selectedProductId <= 0) {
                resetSession(session);
                sendDiscountsDashboard(chatId, "⚠️ Товар для скидки не выбран. Начните заново.");
                return;
            }
            Double newPrice = parsePriceInput(text);
            if (newPrice == null || newPrice <= 0) {
                sendText(chatId,
                        "Введите новую цену числом, например: 1200",
                        inlineSingleColumn(
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
                return;
            }
            applyManualDiscount(chatId, session, newPrice);
            return;
        }

        sendWelcomeMenu(chatId, "Используйте кнопки ниже.");
    }

    private void createProductFromAdminInput(long chatId, AdminSession session, String rawDescription) {
        String article = TextParser.extractArticle(rawDescription);
        if (article == null || !article.matches("\\d{8}")) {
            sendText(chatId,
                    "❗ Не найден корректный артикул (8 цифр).\nПример: Artikal: 56789356",
                    inlineSingleColumn(
                            button("Назад", CB_BACK_TO_PHOTOS),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }
        ProductCard existingByArticle = db.findProductCardByArticle(article);
        if (existingByArticle != null) {
            sendText(chatId,
                    "⚠️ Этот артикул уже используется.\n" +
                            "Товар: " + existingByArticle.title + "\n" +
                            "Статус: " + statusLabel(existingByArticle.status) + "\n" +
                            "Укажите другой артикул.",
                    inlineSingleColumn(
                            button("Назад", CB_BACK_TO_PHOTOS),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }

        String rsd = TextParser.extractRsd(rawDescription);
        if (rsd == null || rsd.isBlank()) {
            sendText(chatId,
                    "❗ Не удалось прочитать цену.\nУкажите строку вида: Cena - 1500 RSD",
                    inlineSingleColumn(
                            button("Назад", CB_BACK_TO_PHOTOS),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }

        double basePrice;
        try {
            basePrice = Double.parseDouble(rsd);
        } catch (NumberFormatException e) {
            sendText(chatId,
                    "❗ Цена имеет неверный формат.\nПроверьте строку Cena.",
                    inlineSingleColumn(
                            button("Назад", CB_BACK_TO_PHOTOS),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }
        if (basePrice <= 0) {
            sendText(chatId,
                    "❗ Цена должна быть больше 0.",
                    inlineSingleColumn(
                            button("Назад", CB_BACK_TO_PHOTOS),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }

        String title = TextParser.extractTitle(rawDescription);
        if (title == null || title.isBlank()) {
            String[] lines = TextParser.normalizeNewlines(rawDescription).split("\\n");
            for (String line : lines) {
                if (!line.trim().isBlank()) {
                    title = line.trim();
                    break;
                }
            }
        }
        if (title == null || title.isBlank()) {
            sendText(chatId,
                    "❗ Не удалось определить название товара.\nДобавьте его в первую строку.",
                    inlineSingleColumn(
                            button("Назад", CB_BACK_TO_PHOTOS),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }
        String size = TextParser.extractSize(rawDescription);

        if (session.pendingPhotoFileIds.isEmpty()) {
            resetSession(session);
            sendPhotoUploadPrompt(chatId, 0);
            return;
        }
        List<String> photoFileIds = new ArrayList<>(session.pendingPhotoFileIds);
        resetSession(session);
        sendText(chatId, "⏳ Публикую товар, подождите 3-10 секунд...");

        final String finalTitle = title;
        final String finalSize = size;
        workers.submit(() -> {
            List<byte[]> images = new ArrayList<>();
            for (String fileId : photoFileIds) {
                try {
                    images.add(downloadFileBytes(fileId));
                } catch (Exception e) {
                    log.warn("Failed to download photo {} for admin product flow", fileId, e);
                }
            }
            if (images.isEmpty()) {
                sendWelcomeMenu(chatId, "❗ Не удалось загрузить фото. Попробуйте добавить товар еще раз.");
                return;
            }

            CategorySelection explicit = TextParser.detectExplicitCategories(rawDescription);
            if (explicit.entries.isEmpty()) {
                explicit.add("Muško", null);
            }
            String caption = buildProductCaption(finalTitle, finalSize, basePrice, basePrice, article, 0, null, "ACTIVE");

            long productId = 0;
            PublishResult pub = null;
            try {
                ProductCard existing = db.findProductCardByArticle(article);
                if (existing != null) {
                    sendWelcomeMenu(chatId,
                            "⚠️ Артикул " + article + " уже существует (" + statusLabel(existing.status) + ").\n" +
                                    "Выберите другой артикул.");
                    return;
                }

                ShopifyClient.ProductPayload payload = new ShopifyClient.ProductPayload();
                payload.title = finalTitle;
                payload.bodyHtml = caption.replace("\n", "<br>");
                payload.priceEur = formatPriceForShopify(basePrice);
                payload.size = finalSize;
                payload.tags = buildTags(explicit, new Classification());
                payload.productType = selectProductType(explicit);
                payload.images = images;
                payload.barcode = article;
                payload.sku = article;

                productId = shopify.createProduct(payload);
                if (config.shopifyPublishAll) {
                    try {
                        shopify.publishProductToAll(productId);
                    } catch (Exception e) {
                        log.warn("Failed to publish product {} to channels", productId, e);
                    }
                }
                for (CategorySelection.Entry entry : explicit.entries) {
                    for (String titleKey : collections.buildCollectionTitles(entry.section, entry.subcategory)) {
                        Long id = collections.getCollectionId(titleKey);
                        if (id != null) {
                            try {
                                shopify.addProductToCollection(productId, id);
                            } catch (Exception e) {
                                log.warn("Failed to add product {} to collection {}", productId, titleKey, e);
                            }
                        }
                    }
                }

                pub = publishToListingChat(caption, images);
                String telegramLink = buildTelegramLink(pub.channelId, pub.primaryMessageId);
                if (telegramLink != null && !telegramLink.isBlank()) {
                    try {
                        shopify.setProductMetafield(productId,
                                config.telegramLinkMetafieldNamespace,
                                config.telegramLinkMetafieldKey,
                                telegramLink,
                                config.telegramLinkMetafieldType);
                    } catch (Exception e) {
                        log.warn("Failed to set telegram link metafield for product {}", productId, e);
                    }
                }

                for (Message sent : pub.messages) {
                    String msgText = sent.getCaption() == null ? "" : sent.getCaption();
                    long sentDate = sent.getDate() == null ? Instant.now().getEpochSecond() : sent.getDate();
                    db.upsertPost(pub.channelId, sent.getMessageId(), pub.mediaGroupId, msgText, sentDate);
                    db.setProductIdForMessage(pub.channelId, sent.getMessageId(), productId);
                }
                if (pub.mediaGroupId != null && !pub.mediaGroupId.isBlank()) {
                    db.upsertMediaGroup(pub.mediaGroupId, pub.channelId);
                    db.markMediaGroupProcessed(pub.mediaGroupId);
                }

                db.upsertProductCard(new ProductCard(
                        productId,
                        pub.channelId,
                        pub.primaryMessageId,
                        pub.mediaGroupId,
                        finalTitle,
                        finalSize,
                        rawDescription,
                        article,
                        basePrice,
                        basePrice,
                        0,
                        null,
                        "ACTIVE",
                        Instant.now().getEpochSecond(),
                        Instant.now().getEpochSecond()
                ));

                sendWelcomeMenu(chatId,
                        "✅ Товар опубликован.\nНазвание: " + finalTitle + "\nЦена: " + formatRsd(basePrice) + " RSD\nАртикул: " + article);
            } catch (Exception e) {
                log.error("Failed to create product from admin flow", e);
                String errorText = e.getMessage() == null ? "" : e.getMessage();
                boolean articleConflict = errorText.contains("product_cards.article") || errorText.contains("DB upsertProductCard failed");
                if (productId > 0) {
                    try {
                        shopify.deleteProduct(productId);
                    } catch (Exception ignored) {
                    }
                }
                if (pub != null) {
                    deleteTelegramByReference(pub.channelId, pub.primaryMessageId, pub.mediaGroupId);
                }
                if (articleConflict) {
                    sendWelcomeMenu(chatId,
                            "⚠️ Артикул уже существует в базе.\n" +
                                    "Товар не создан. Используйте другой артикул из 8 цифр.");
                } else {
                    sendWelcomeMenu(chatId, "❌ Ошибка публикации товара: " + e.getMessage());
                }
            }
        });
    }

    private PublishResult publishToListingChat(String caption, List<byte[]> images) throws TelegramApiException {
        if (images.size() == 1) {
            SendPhoto sendPhoto = new SendPhoto();
            sendPhoto.setChatId(config.telegramPublishChatId);
            if (config.telegramPublishThreadId != null && config.telegramPublishThreadId > 0) {
                sendPhoto.setMessageThreadId(config.telegramPublishThreadId);
            }
            sendPhoto.setCaption(caption);
            sendPhoto.setPhoto(new InputFile(new ByteArrayInputStream(images.get(0)), "photo-1.jpg"));
            Message sent = execute(sendPhoto);
            return new PublishResult(
                    String.valueOf(sent.getChatId()),
                    sent.getMessageId(),
                    sent.getMediaGroupId(),
                    List.of(sent)
            );
        }

        SendMediaGroup mediaGroup = new SendMediaGroup();
        mediaGroup.setChatId(config.telegramPublishChatId);
        if (config.telegramPublishThreadId != null && config.telegramPublishThreadId > 0) {
            mediaGroup.setMessageThreadId(config.telegramPublishThreadId);
        }
        List<InputMedia> media = new ArrayList<>();
        for (int i = 0; i < images.size(); i++) {
            InputMediaPhoto photo = new InputMediaPhoto();
            photo.setMedia(new ByteArrayInputStream(images.get(i)), "photo-" + (i + 1) + ".jpg");
            if (i == 0) {
                photo.setCaption(caption);
            }
            media.add(photo);
        }
        mediaGroup.setMedias(media);
        List<Message> sent = execute(mediaGroup);
        Message first = sent.get(0);
        return new PublishResult(
                String.valueOf(first.getChatId()),
                first.getMessageId(),
                first.getMediaGroupId(),
                sent
        );
    }

    private void sendProductsPage(long chatId, int page) {
        int pageSize = Math.max(1, config.listPageSize);
        int total = db.countVisibleProducts();
        int pages = Math.max(1, (int) Math.ceil(total / (double) pageSize));
        int maxPage = pages - 1;
        int safePage = Math.max(0, Math.min(page, maxPage));
        List<ProductCard> items = db.listVisibleProducts(pageSize, safePage * pageSize);

        StringBuilder sb = new StringBuilder();
        sb.append("📦 Активные товары: ").append(total).append("\n")
                .append("Страница ").append(safePage + 1).append("/").append(pages).append("\n");
        if (items.isEmpty()) {
            sb.append("Список пуст.");
        } else {
            int startOrdinal = safePage * pageSize + 1;
            for (int i = 0; i < items.size(); i++) {
                ProductCard card = items.get(i);
                sb.append("\n")
                        .append(startOrdinal + i)
                        .append(". ")
                        .append(card.title)
                        .append(" | ")
                        .append(formatRsd(card.currentPriceRsd))
                        .append(" RSD")
                        .append(" | Artikal: ")
                        .append(card.article)
                        .append(" | ")
                        .append("RESERVED".equals(card.status) ? "REZERVISANO" : "AKTIVAN");
            }
        }
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        appendPaginationRows(rows, "PRODUCTS", safePage, pages);
        rows.add(List.of(button("⬅ Назад в меню", CB_MENU)));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        markup.setKeyboard(rows);
        sendText(chatId, sb.toString(), markup);
    }

    private void sendUsersPage(long chatId, int page) {
        int pageSize = Math.max(1, config.listPageSize);
        int total = db.countUsers();
        int pages = Math.max(1, (int) Math.ceil(total / (double) pageSize));
        int maxPage = pages - 1;
        int safePage = Math.max(0, Math.min(page, maxPage));
        List<UserRecord> users = db.listUsers(pageSize, safePage * pageSize);

        StringBuilder sb = new StringBuilder();
        sb.append("👥 Пользователи: ").append(total).append("\n")
                .append("Страница ").append(safePage + 1).append("/").append(pages).append("\n");
        if (users.isEmpty()) {
            sb.append("Список пуст.");
        } else {
            int startOrdinal = safePage * pageSize + 1;
            for (int i = 0; i < users.size(); i++) {
                UserRecord u = users.get(i);
                String label = (u.firstName == null ? "" : u.firstName) + (u.lastName == null ? "" : (" " + u.lastName));
                label = label.trim();
                if (label.isBlank()) {
                    label = u.username == null || u.username.isBlank() ? String.valueOf(u.userId) : "@" + u.username;
                }
                sb.append("\n")
                        .append(startOrdinal + i)
                        .append(". ")
                        .append(label)
                        .append(" (")
                        .append(u.userId)
                        .append(")")
                        .append(u.admin ? " [ADMIN]" : "");
            }
        }
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        appendPaginationRows(rows, "USERS", safePage, pages);
        rows.add(List.of(button("⬅ Назад в меню", CB_MENU)));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        markup.setKeyboard(rows);
        sendText(chatId, sb.toString(), markup);
    }

    private void sendSelectableProductsPage(long chatId, AdminSession session, int page) {
        int pageSize = Math.max(1, config.listPageSize);
        boolean reservedOnly = session.state == AdminState.UNRESERVE_SELECT;
        int total = reservedOnly ? db.countReservedProducts() : db.countVisibleProducts();
        int pages = Math.max(1, (int) Math.ceil(total / (double) pageSize));
        int maxPage = pages - 1;
        int safePage = Math.max(0, Math.min(page, maxPage));

        List<ProductCard> items = reservedOnly
                ? db.listReservedProducts(pageSize, safePage * pageSize)
                : db.listVisibleProducts(pageSize, safePage * pageSize);

        StringBuilder sb = new StringBuilder();
        if (session.state == AdminState.RESERVE_SELECT) {
            sb.append("🟡 Выберите товар для резерва.");
        } else if (session.state == AdminState.UNRESERVE_SELECT) {
            sb.append("🟢 Выберите товар для снятия резерва.");
        } else if (session.state == AdminState.MANUAL_DISCOUNT_SELECT) {
            sb.append("🏷 Выберите товар для ручной скидки.");
        } else {
            sb.append("✅ Выберите товар для отметки \"Продано\".");
        }
        sb.append("\nВсего: ").append(total).append("\n");
        sb.append("Страница ").append(safePage + 1).append("/").append(pages).append("\n");
        sb.append("Введите номер позиции цифрой.\n");

        if (items.isEmpty()) {
            sb.append("\nСписок пуст.");
        } else {
            int startOrdinal = safePage * pageSize + 1;
            for (int i = 0; i < items.size(); i++) {
                ProductCard card = items.get(i);
                sb.append("\n")
                        .append(startOrdinal + i)
                        .append(". ")
                        .append(card.title)
                        .append(" | ")
                        .append(formatRsd(card.currentPriceRsd))
                        .append(" RSD | Artikal: ")
                        .append(card.article);
            }
        }

        String scope = session.state == AdminState.RESERVE_SELECT ? "RESERVE"
                : session.state == AdminState.UNRESERVE_SELECT ? "UNRESERVE"
                : session.state == AdminState.MANUAL_DISCOUNT_SELECT ? "MANUAL"
                : "SOLD";
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        appendPaginationRows(rows, scope, safePage, pages);
        rows.add(List.of(button("⬅ Назад в меню", CB_MENU)));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        markup.setKeyboard(rows);
        sendText(chatId, sb.toString(), markup);
    }

    private void appendPaginationRows(List<List<InlineKeyboardButton>> rows, String scope, int page, int pages) {
        if (pages <= 1) return;
        if (page > 0) {
            rows.add(List.of(button("⬅ Предыдущая страница", "PAGE:" + scope + ":" + (page - 1))));
        }
        rows.add(List.of(button("Страница " + (page + 1) + "/" + pages, CB_NOOP)));
        if (page + 1 < pages) {
            rows.add(List.of(button("Следующая страница ➡", "PAGE:" + scope + ":" + (page + 1))));
        }
    }

    private void markCardAsSold(ProductCard card) throws IOException {
        String soldCaption = "PRODATO\n" + buildProductCaption(
                card.title,
                card.size,
                card.basePriceRsd,
                card.currentPriceRsd,
                card.article,
                card.discountPercent,
                card.fixedPriceRsd,
                "SOLD"
        );
        try {
            editTelegramCaption(card.channelId, card.messageId, soldCaption);
        } catch (Exception e) {
            log.warn("Failed to put PRODATO caption for product {}", card.productId, e);
        }
        try {
            shopify.deleteProduct(card.productId);
        } catch (Exception e) {
            log.warn("Failed to delete product {} from Shopify", card.productId, e);
        }
        db.markProductStatus(card.productId, "SOLD");
        db.deleteProductCard(card.productId);
        deleteTelegramByReference(card.channelId, card.messageId, card.mediaGroupId);
    }

    private void syncCardState(ProductCard card, String status, int discountPercent, Double fixedPriceRsd, double currentPriceRsd) throws IOException {
        ShopifyProductSnapshot snapshot = shopify.getProductSnapshot(card.productId);
        String shopifyTitle = "RESERVED".equals(status) ? ("REZERVISANO | " + card.title) : card.title;
        String caption = buildProductCaption(card.title, card.size, card.basePriceRsd, currentPriceRsd, card.article, discountPercent, fixedPriceRsd, status);
        String bodyHtml = caption.replace("\n", "<br>");

        shopify.updateProduct(
                card.productId,
                snapshot.variantId,
                shopifyTitle,
                bodyHtml,
                formatPriceForShopify(currentPriceRsd),
                card.size,
                card.article
        );
        syncSaleCollection(card.productId, discountPercent > 0 || fixedPriceRsd != null);
        try {
            editTelegramCaption(card.channelId, card.messageId, caption);
        } catch (TelegramApiException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
            if (!msg.contains("message is not modified")) {
                throw new IOException("Failed to edit Telegram caption", e);
            }
        }
        db.updatePostText(card.channelId, card.messageId, caption);
        db.updateProductCardPricing(card.productId, currentPriceRsd, discountPercent, fixedPriceRsd);
        db.updateProductCardStatus(card.productId, status);
    }

    private void syncSaleCollection(long productId, boolean addToSale) {
        Long saleCollectionId = collections.getCollectionId("Sniženje");
        if (saleCollectionId == null) return;
        try {
            if (addToSale) {
                shopify.addProductToCollection(productId, saleCollectionId);
            } else {
                shopify.removeProductFromCollection(saleCollectionId, productId);
            }
        } catch (Exception e) {
            log.warn("Failed to sync Sniženje collection for product {}", productId, e);
        }
    }

    private void editTelegramCaption(String channelId, long messageId, String caption) throws TelegramApiException {
        EditMessageCaption edit = new EditMessageCaption();
        edit.setChatId(channelId);
        edit.setMessageId((int) messageId);
        edit.setCaption(caption);
        execute(edit);
    }

    private String buildProductCaption(String title,
                                       String size,
                                       double basePriceRsd,
                                       double currentPriceRsd,
                                       String article,
                                       int discountPercent,
                                       Double fixedPriceRsd,
                                       String status) {
        List<String> lines = new ArrayList<>();
        if ("RESERVED".equals(status)) {
            lines.add("REZERVISANO");
        }
        if ("SOLD".equals(status)) {
            lines.add("PRODATO");
        }
        if (fixedPriceRsd != null) {
            lines.add("SNIŽENJE " + formatRsd(basePriceRsd) + " -> " + formatRsd(fixedPriceRsd) + " RSD");
        } else if (discountPercent > 0) {
            lines.add("SNIŽENJE " + formatRsd(basePriceRsd) + "-" + discountPercent + "%=" + formatRsd(currentPriceRsd));
        }
        lines.add(title);
        if (size != null && !size.isBlank()) {
            lines.add("Vel - " + size);
        }
        lines.add("Cena - " + formatRsd(currentPriceRsd) + " RSD");
        lines.add("Artikal: " + article);
        return String.join("\n", lines) + "\n\n" + ORDER_CONTACT_FOOTER;
    }

    private String formatPriceForShopify(double value) {
        if (Math.abs(value - Math.round(value)) < 0.00001) {
            return String.valueOf((long) Math.round(value));
        }
        return String.format(Locale.US, "%.2f", value);
    }

    private String formatRsd(double value) {
        if (Math.abs(value - Math.round(value)) < 0.00001) {
            return String.valueOf((long) Math.round(value));
        }
        return String.format(Locale.US, "%.2f", value);
    }

    private String statusLabel(String status) {
        if ("ACTIVE".equals(status)) return "AKTIVAN";
        if ("RESERVED".equals(status)) return "REZERVISANO";
        if ("SOLD".equals(status)) return "PRODATO";
        if ("OUT_OF_STOCK".equals(status)) return "RASPRODATO";
        if ("MISSING".equals(status)) return "OBRISANO";
        return status == null ? "-" : status;
    }

    private void sendWelcomeMenu(long chatId) {
        sendWelcomeMenu(chatId, null);
    }

    private void sendWelcomeMenu(long chatId, String status) {
        StringBuilder text = new StringBuilder();
        text.append("✨ Панель управления SecondHand Ogledalo\n");
        text.append("Выберите действие из меню ниже.");
        if (status != null && !status.isBlank()) {
            text.append("\n\n").append(status);
        }
        sendText(chatId, text.toString(), buildMainInlineKeyboard());
    }

    private void sendDiscountsDashboard(long chatId) {
        sendDiscountsDashboard(chatId, null);
    }

    private void sendDiscountsDashboard(long chatId, String statusMessage) {
        LocalDate today = LocalDate.now(discountZone == null ? ZoneId.systemDefault() : discountZone);
        boolean enabled = isDiscountsEnabled();
        String phase = describeDiscountStage(today);
        String dateLabel = today.format(DateTimeFormatter.ofPattern("dd.MM.yyyy"));

        StringBuilder text = new StringBuilder();
        text.append("🧾 Скидки\n");
        text.append("Статус: ").append(enabled ? "включены ✅" : "отключены ⏸").append("\n");
        text.append("Сегодня: ").append(dateLabel).append("\n");
        text.append("Текущий этап: ").append(phase).append("\n");
        text.append("\n");
        text.append("Автосистема применяет изменения 1 раз в день.\n");
        text.append("Базовая прогрессия по возрасту: 0% → 15% → 30% → 50%.\n");
        text.append("Последняя неделя месяца: Пн 20%, Вт 30%, Ср 40%, Чт 50%, Пт 500, Сб/Вс 350.");
        if (statusMessage != null && !statusMessage.isBlank()) {
            text.append("\n\n").append(statusMessage);
        }

        InlineKeyboardButton toggle = enabled
                ? button("⏸ Отключить скидки", CB_DISCOUNTS_DISABLE)
                : button("▶️ Включить скидки", CB_DISCOUNTS_ENABLE);
        sendText(chatId, text.toString(), inlineSingleColumn(
                toggle,
                button("🏷 Скидка на товар", CB_MANUAL_DISCOUNT),
                button("⬅ Назад в меню", CB_MENU)
        ));
    }

    private boolean isDiscountsEnabled() {
        String value = db.getMeta(META_DISCOUNT_ENABLED);
        if (value == null || value.isBlank()) return true;
        return !"false".equalsIgnoreCase(value.trim());
    }

    private LocalDate todayInDiscountZone() {
        return LocalDate.now(discountZone == null ? ZoneId.systemDefault() : discountZone);
    }

    private LocalDate getDiscountResetStartDate() {
        String raw = db.getMeta(META_DISCOUNT_RESET_START);
        if (raw == null || raw.isBlank()) return null;
        try {
            return LocalDate.parse(raw.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private String describeDiscountStage(LocalDate today) {
        LocalDate resetStart = getDiscountResetStartDate();
        if (resetStart != null && !today.isBefore(resetStart)) {
            long resetDays = Math.max(0, java.time.temporal.ChronoUnit.DAYS.between(resetStart, today));
            if (resetDays < 7) return "цикл после включения: неделя без скидок";
        }
        LocalDate monthEnd = YearMonth.from(today).atEndOfMonth();
        LocalDate lastWeekStart = monthEnd.with(java.time.temporal.TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY));
        if (today.isBefore(lastWeekStart)) {
            return "прогрессивный этап по возрасту товара";
        }
        int dow = today.getDayOfWeek().getValue(); // Mon=1
        if (dow == 1) return "последняя неделя: минимум 20%";
        if (dow == 2) return "последняя неделя: минимум 30%";
        if (dow == 3) return "последняя неделя: минимум 40%";
        if (dow == 4) return "последняя неделя: минимум 50%";
        if (dow == 5) return "последняя неделя: все по 500";
        return "последняя неделя: все по 350";
    }

    private void sendPhotoUploadPrompt(long chatId, int photoCount) {
        String text = "📸 Добавление товара\n\n" +
                "Загрузите до 9 фото.\n" +
                "Принято: " + photoCount + "/9.";
        if (photoCount <= 0) {
            text += "\n\nОтправьте первое фото, после этого появится кнопка «Готово».";
            sendText(chatId, text, inlineSingleColumn(
                    button("Отменить", CB_CANCEL_FLOW)
            ));
            return;
        }
        text += "\n\nЕсли фото достаточно, нажмите «Готово».";
        sendText(chatId, text, inlineSingleColumn(
                button("Готово", CB_DONE_PHOTOS),
                button("Отменить", CB_CANCEL_FLOW)
        ));
    }

    private void sendText(long chatId, String text) {
        sendText(chatId, text, null);
    }

    private void sendText(long chatId, String text, InlineKeyboardMarkup inlineKeyboard) {
        SendMessage msg = new SendMessage();
        msg.setChatId(chatId);
        msg.setText(text);
        if (inlineKeyboard != null) {
            msg.setReplyMarkup(inlineKeyboard);
        }
        try {
            execute(msg);
        } catch (TelegramApiException e) {
            log.warn("Failed to send message to {}: {}", chatId, e.getMessage());
        }
    }

    private InlineKeyboardMarkup buildMainInlineKeyboard() {
        return inlineSingleColumn(
                button("➕ Добавить товар", CB_ADD_PRODUCT),
                button("📦 Список товаров", CB_PRODUCTS),
                button("🟡 Зарезервировать", CB_RESERVE),
                button("🟢 Снять резерв", CB_UNRESERVE),
                button("✅ Продано", CB_SOLD)
        );
    }

    private InlineKeyboardButton button(String text, String callbackData) {
        InlineKeyboardButton button = new InlineKeyboardButton();
        button.setText(text);
        button.setCallbackData(callbackData);
        return button;
    }

    @SafeVarargs
    private final InlineKeyboardMarkup inlineSingleColumn(InlineKeyboardButton... buttons) {
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        for (InlineKeyboardButton button : buttons) {
            rows.add(List.of(button));
        }
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        markup.setKeyboard(rows);
        return markup;
    }

    private void answerCallback(CallbackQuery callback, String text) {
        try {
            AnswerCallbackQuery answer = new AnswerCallbackQuery();
            answer.setCallbackQueryId(callback.getId());
            answer.setText(text);
            answer.setShowAlert(false);
            execute(answer);
        } catch (Exception ignored) {
        }
    }

    private AdminSession sessionFor(long userId) {
        return sessions.computeIfAbsent(userId, k -> new AdminSession());
    }

    private void resetSession(AdminSession session) {
        session.state = AdminState.IDLE;
        session.pendingPhotoFileIds.clear();
        session.selectedProductId = 0;
    }

    private void syncDiscountsSafe() {
        try {
            syncDiscounts();
        } catch (Exception e) {
            log.warn("Discount sync failed", e);
        }
    }

    private void syncDeletedProductsSafe() {
        try {
            syncDeletedProducts();
        } catch (Exception e) {
            log.warn("Product availability sync failed", e);
        }
    }

    private Double parsePriceInput(String text) {
        if (text == null) return null;
        String cleaned = text.trim()
                .replace("RSD", "")
                .replace("rsd", "")
                .replace("дин", "")
                .replace("din", "")
                .replace(" ", "");
        cleaned = cleaned.replace(",", ".");
        cleaned = cleaned.replaceAll("[^0-9.]", "");
        if (cleaned.isBlank()) return null;
        try {
            return Double.parseDouble(cleaned);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void applyManualDiscount(long chatId, AdminSession session, double newPrice) {
        ProductCard card = db.findProductCardById(session.selectedProductId);
        if (card == null || (!"ACTIVE".equals(card.status) && !"RESERVED".equals(card.status))) {
            resetSession(session);
            sendDiscountsDashboard(chatId, "⚠️ Товар не найден среди активных. Попробуйте снова.");
            return;
        }
        try {
            int discountPercent = discountPercentByFixed(card.basePriceRsd, newPrice);
            syncCardState(card, card.status, discountPercent, newPrice, newPrice);
            session.state = AdminState.MANUAL_DISCOUNT_SELECT;
            session.selectedProductId = 0;
            sendText(chatId,
                    "✅ Цена обновлена: " + card.title + "\nНовая цена: " + formatRsd(newPrice) + " RSD\nТовар помечен как SNIŽENJE.");
            sendSelectableProductsPage(chatId, session, 0);
        } catch (Exception e) {
            log.warn("Failed to apply manual discount for product {}", card.productId, e);
            sendText(chatId, "❌ Не удалось применить скидку: " + e.getMessage());
        }
    }

    private void syncDiscounts() {
        if (!isDiscountsEnabled()) {
            return;
        }
        LocalDate today = todayInDiscountZone();
        String key = "discount:last_sync_date";
        String lastDate = db.getMeta(key);
        if (today.toString().equals(lastDate)) {
            return;
        }

        List<ProductCard> cards = db.listProductsForDiscount();
        if (cards.isEmpty()) {
            db.setMeta(key, today.toString());
            return;
        }

        for (ProductCard card : cards) {
            try {
                DiscountTarget target = calculateDiscountTarget(card, today);
                if (target == null) continue;
                boolean samePrice = Math.abs(card.currentPriceRsd - target.currentPriceRsd) < 0.00001;
                boolean sameDiscount = card.discountPercent == target.discountPercent;
                boolean sameFixed = (card.fixedPriceRsd == null && target.fixedPriceRsd == null) ||
                        (card.fixedPriceRsd != null && target.fixedPriceRsd != null &&
                                Math.abs(card.fixedPriceRsd - target.fixedPriceRsd) < 0.00001);
                if (samePrice && sameDiscount && sameFixed) {
                    continue;
                }
                syncCardState(card, card.status, target.discountPercent, target.fixedPriceRsd, target.currentPriceRsd);
            } catch (Exception e) {
                log.warn("Failed to apply discount to product {}", card.productId, e);
            }
        }
        db.setMeta(key, today.toString());
    }

    private DiscountTarget calculateDiscountTarget(ProductCard card, LocalDate today) {
        if (card.basePriceRsd <= 0) return null;

        if (card.fixedPriceRsd != null && card.fixedPriceRsd <= 350.0) {
            return new DiscountTarget(discountPercentByFixed(card.basePriceRsd, 350.0), 350.0, 350.0);
        }

        LocalDate createdAtDate = Instant.ofEpochSecond(card.createdAt)
                .atZone(discountZone == null ? ZoneId.systemDefault() : discountZone)
                .toLocalDate();
        LocalDate resetStart = getDiscountResetStartDate();
        boolean hadDiscountBeforeReset = resetStart != null
                && createdAtDate.isBefore(resetStart)
                && (card.fixedPriceRsd != null || card.discountPercent > 0);
        if (hadDiscountBeforeReset) {
            return new DiscountTarget(card.discountPercent, card.currentPriceRsd, card.fixedPriceRsd);
        }
        LocalDate ageStart = createdAtDate;
        if (resetStart != null && resetStart.isAfter(ageStart) && (card.fixedPriceRsd == null && card.discountPercent <= 0)) {
            ageStart = resetStart;
        }
        long ageDays = Math.max(0, java.time.temporal.ChronoUnit.DAYS.between(ageStart, today));
        int ageWeeks = (int) (ageDays / 7L);
        int discount = ageWeeks <= 0 ? 0 : ageWeeks == 1 ? 15 : ageWeeks == 2 ? 30 : 50;

        YearMonth ym = YearMonth.from(today);
        Double fixed = null;

        LocalDate monthEnd = ym.atEndOfMonth();
        LocalDate lastWeekStart = monthEnd.with(java.time.temporal.TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY));
        boolean firstResetWeek = resetStart != null && !today.isBefore(resetStart) && ageDays < 7;
        if (!today.isBefore(lastWeekStart) && !firstResetWeek) {
            int dow = today.getDayOfWeek().getValue(); // Mon=1
            if (dow == 5) {
                fixed = 500.0;
            } else if (dow >= 6) {
                fixed = 350.0;
            } else {
                int minDiscount;
                if (dow == 1) {
                    minDiscount = 20;
                } else if (dow == 2) {
                    minDiscount = 30;
                } else if (dow == 3) {
                    minDiscount = 40;
                } else {
                    minDiscount = 50;
                }
                discount = Math.max(discount, minDiscount);
            }
        }

        double price;
        int effectiveDiscount;
        if (fixed != null) {
            price = fixed;
            effectiveDiscount = discountPercentByFixed(card.basePriceRsd, fixed);
        } else {
            price = Math.max(1, Math.round(card.basePriceRsd * (100.0 - discount) / 100.0));
            effectiveDiscount = discount;
        }
        return new DiscountTarget(effectiveDiscount, price, fixed);
    }

    private int discountPercentByFixed(double basePrice, double fixedPrice) {
        if (basePrice <= 0) return 0;
        return Math.max(0, (int) Math.round((1.0 - (fixedPrice / basePrice)) * 100.0));
    }

    private static class PublishResult {
        final String channelId;
        final long primaryMessageId;
        final String mediaGroupId;
        final List<Message> messages;

        PublishResult(String channelId, long primaryMessageId, String mediaGroupId, List<Message> messages) {
            this.channelId = channelId;
            this.primaryMessageId = primaryMessageId;
            this.mediaGroupId = mediaGroupId;
            this.messages = messages == null ? Collections.emptyList() : messages;
        }
    }

    private static class DiscountTarget {
        final int discountPercent;
        final double currentPriceRsd;
        final Double fixedPriceRsd;

        DiscountTarget(int discountPercent, double currentPriceRsd, Double fixedPriceRsd) {
            this.discountPercent = discountPercent;
            this.currentPriceRsd = currentPriceRsd;
            this.fixedPriceRsd = fixedPriceRsd;
        }
    }

    private enum AdminState {
        IDLE,
        ADD_PRODUCT_PHOTOS,
        ADD_PRODUCT_DESCRIPTION,
        ADD_ADMIN_ID,
        RESERVE_SELECT,
        UNRESERVE_SELECT,
        SOLD_SELECT,
        MANUAL_DISCOUNT_SELECT,
        MANUAL_DISCOUNT_INPUT
    }

    private static class AdminSession {
        AdminState state = AdminState.IDLE;
        final List<String> pendingPhotoFileIds = new ArrayList<>();
        long selectedProductId = 0;
    }

    private void handleMessage(Message message, boolean isEdit) {
        String channelId = String.valueOf(message.getChatId());
        if (config.telegramChannelId != null && !config.telegramChannelId.isBlank() && !config.telegramChannelId.equals(channelId)) {
            log.info("Ignored message {} from channel {} (expected {})", message.getMessageId(), channelId, config.telegramChannelId);
            return;
        }
        if (message.getFrom() != null && Boolean.TRUE.equals(message.getFrom().getIsBot())) {
            log.info("Ignored bot-authored message {} in {}", message.getMessageId(), channelId);
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

            if (existingProductId != null) {
                Long productId = existingProductId;
                String mediaGroupId = message.getMediaGroupId();
                long msgId = message.getMessageId();
                workers.submit(() -> updateTelegramLinkMetafield(productId, channelId, msgId, mediaGroupId));
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
        String article = TextParser.extractArticle(text);
        if (article != null && !article.isBlank()) {
            payload.barcode = article;
            payload.sku = article;
        }
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
                        config.telegramLinkMetafieldType);
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
            return "https://t.me/" + clean + "/" + messageId + "?single";
        }
        String normalized = channelId.trim();
        if (normalized.startsWith("-100")) {
            normalized = normalized.substring(4);
        } else if (normalized.startsWith("-")) {
            normalized = normalized.substring(1);
        }
        if (normalized.isBlank()) return null;
        return "https://t.me/c/" + normalized + "/" + messageId + "?single";
    }

    private void updateTelegramLinkMetafield(long productId, String channelId, Long messageId, String mediaGroupId) {
        Long resolvedMessageId = messageId;
        if ((resolvedMessageId == null || resolvedMessageId <= 0) && mediaGroupId != null && !mediaGroupId.isBlank()) {
            List<Long> ids = db.listMessageIdsForMediaGroup(mediaGroupId);
            if (!ids.isEmpty()) {
                resolvedMessageId = ids.get(0);
            }
        }
        String telegramLink = buildTelegramLink(channelId, resolvedMessageId);
        if (telegramLink == null || telegramLink.isBlank()) return;
        try {
            shopify.setProductMetafield(productId,
                    config.telegramLinkMetafieldNamespace,
                    config.telegramLinkMetafieldKey,
                    telegramLink,
                    config.telegramLinkMetafieldType);
        } catch (Exception e) {
            log.warn("Failed to set telegram link metafield for product {}", productId, e);
        }
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
            db.deleteProductCard(productId);
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
            String article = TextParser.extractArticle(text);

            String telegramLink = buildTelegramLink(channelId, messageId);
            String bodyHtml = buildDescription(text, "", priceSelection, discount, telegramLink);
            ShopifyProductSnapshot snap = shopify.getProductSnapshot(productId);
            shopify.updateProduct(productId, snap.variantId, title, bodyHtml, priceSelection.price, size, article);
            log.info("Product {} updated from edited message {}", productId, messageId);
            if (telegramLink != null && !telegramLink.isBlank()) {
                try {
                    shopify.setProductMetafield(productId,
                            config.telegramLinkMetafieldNamespace,
                            config.telegramLinkMetafieldKey,
                            telegramLink,
                            config.telegramLinkMetafieldType);
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
                ShopifyClient.ProductAvailability availability = shopify.getProductAvailability(ref.productId);
                if (availability == ShopifyClient.ProductAvailability.MISSING) {
                    deleteTelegramByReference(ref.channelId, ref.messageId, ref.mediaGroupId);
                    db.markProductStatus(ref.productId, "MISSING");
                    db.deleteProductCard(ref.productId);
                    log.info("Product missing in Shopify, deleted Telegram message(s). productId={}", ref.productId);
                } else if (availability == ShopifyClient.ProductAvailability.OUT_OF_STOCK) {
                    try {
                        shopify.deleteProduct(ref.productId);
                        log.info("Product {} deleted in Shopify after reaching 0 stock", ref.productId);
                    } catch (Exception e) {
                        log.warn("Failed to delete out-of-stock product {} in Shopify", ref.productId, e);
                    }
                    deleteTelegramByReference(ref.channelId, ref.messageId, ref.mediaGroupId);
                    db.markProductStatus(ref.productId, "OUT_OF_STOCK");
                    db.deleteProductCard(ref.productId);
                    log.info("Product out of stock in Shopify, deleted Telegram message(s). productId={}", ref.productId);
                }
                if (config.productSyncDelayMs > 0) {
                    Thread.sleep(config.productSyncDelayMs);
                }
            } catch (RateLimitException e) {
                log.warn("Rate limited by Shopify during sync (productId={}), pausing until next cycle", ref.productId);
                return;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("Failed to sync product {} existence", ref.productId, e);
            }
        }
    }

    private void processSelectionByOrdinal(long chatId, AdminSession session, int ordinal) {
        if (ordinal <= 0) {
            sendText(chatId, "Номер должен быть больше 0.");
            return;
        }
        ProductCard card;
        if (session.state == AdminState.UNRESERVE_SELECT) {
            card = db.findReservedProductByOrdinal(ordinal);
        } else {
            card = db.findVisibleProductByOrdinal(ordinal);
        }
        if (card == null) {
            sendText(chatId, "Позиция не найдена. Проверьте номер и попробуйте снова.");
            return;
        }

        try {
            if (session.state == AdminState.RESERVE_SELECT) {
                if ("RESERVED".equals(card.status)) {
                    sendText(chatId, "Этот товар уже зарезервирован.");
                    return;
                }
                syncCardState(card, "RESERVED", card.discountPercent, card.fixedPriceRsd, card.currentPriceRsd);
                sendText(chatId, "✅ Резерв поставлен: " + card.title + " (Artikal: " + card.article + ")");
                sendSelectableProductsPage(chatId, session, 0);
                return;
            }
            if (session.state == AdminState.UNRESERVE_SELECT) {
                syncCardState(card, "ACTIVE", card.discountPercent, card.fixedPriceRsd, card.currentPriceRsd);
                sendText(chatId, "✅ Резерв снят: " + card.title + " (Artikal: " + card.article + ")");
                sendSelectableProductsPage(chatId, session, 0);
                return;
            }
            if (session.state == AdminState.SOLD_SELECT) {
                markCardAsSold(card);
                sendText(chatId, "✅ Товар помечен проданным: " + card.title + " (Artikal: " + card.article + ")");
                sendSelectableProductsPage(chatId, session, 0);
                return;
            }
            if (session.state == AdminState.MANUAL_DISCOUNT_SELECT) {
                session.selectedProductId = card.productId;
                session.state = AdminState.MANUAL_DISCOUNT_INPUT;
                sendText(chatId,
                        "Введите новую цену для товара:\n" + card.title + "\nТекущая цена: " + formatRsd(card.currentPriceRsd) + " RSD",
                        inlineSingleColumn(
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
            }
        } catch (Exception e) {
            log.warn("Failed to process selection by ordinal for product {}", card.productId, e);
            sendText(chatId, "❌ Не удалось выполнить действие: " + e.getMessage());
        }
    }

    @Override
    public String getBotUsername() {
        return "ShopifyBridgeBot";
    }
}
