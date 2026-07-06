package com.shopifybot.telegram;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.shopifybot.Config;
import com.shopifybot.ai.CategorySelection;
import com.shopifybot.ai.Classification;
import com.shopifybot.ai.KieAiClient;
import com.shopifybot.db.Database;
import com.shopifybot.db.Database.ProductCard;
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
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShopifyBot extends TelegramLongPollingBot {
    private static final Logger log = LoggerFactory.getLogger(ShopifyBot.class);
    private static final String ORDER_CONTACT_FOOTER = "Ako želite da naručite neku stvar, pošaljite fotografiju ove stvari @alinka809 ili @hlestovdmitry";
    private static final Pattern SHOPIFY_ID_FOOTER_PATTERN = Pattern.compile("(?is)(?:<br>\\s*)*ID:\\s*\\d+\\s*$");
    private static final String META_SHOPIFY_SYNC_OFFSET = "shopify:sync_offset";
    private static final String META_SHOPIFY_POS_ONLY_SYNC_OFFSET = "shopify:pos_only_sync_offset";
    private static final String META_SHOPIFY_READ_COOLDOWN_UNTIL = "shopify:read_cooldown_until";
    private static final String CB_MENU = "MENU";
    private static final String CB_NOOP = "NOOP";
    private static final String CB_ADD_PRODUCT = "OPEN:ADD_PRODUCT";
    private static final String CB_ADD_PRODUCT_OLD = "OPEN:ADD_PRODUCT_OLD";
    private static final String CB_ADD_PRODUCT_AI = "OPEN:ADD_PRODUCT_AI";
    private static final String CB_WITHOUT_PHOTO = "OPEN:WITHOUT_PHOTO";
    private static final String CB_SCHEDULED_POSTS = "OPEN:SCHEDULED_POSTS";
    private static final String CB_SCHEDULE_PLAN = "OPEN:SCHEDULE_PLAN";
    private static final String CB_SLOT_ADD = "SLOT:ADD";
    private static final String CB_PRODUCTS = "OPEN:PRODUCTS";
    private static final String CB_RESERVE = "OPEN:RESERVE";
    private static final String CB_UNRESERVE = "OPEN:UNRESERVE";
    private static final String CB_SOLD = "OPEN:SOLD";
    private static final String CB_EDIT_POST = "OPEN:EDIT_POST";
    private static final String CB_USERS = "OPEN:USERS";
    private static final String CB_ADD_ADMIN = "OPEN:ADD_ADMIN";
    private static final String CB_DISCOUNTS = "OPEN:DISCOUNTS";
    private static final String CB_DISCOUNTS_DISABLE = "DISCOUNT:DISABLE";
    private static final String CB_DISCOUNTS_ENABLE = "DISCOUNT:ENABLE";
    private static final String CB_DISCOUNTS_RESET = "DISCOUNT:RESET";
    private static final String CB_DISCOUNTS_RESET_CONFIRM = "DISCOUNT:RESET_CONFIRM";
    private static final String CB_DISCOUNTS_SET_DATE = "DISCOUNT:SET_DATE";
    private static final String CB_MANUAL_DISCOUNT = "OPEN:MANUAL_DISCOUNT";
    private static final String CB_DONE_PHOTOS = "FLOW:DONE_PHOTOS";
    private static final String CB_BACK_TO_PHOTOS = "FLOW:BACK_TO_PHOTOS";
    private static final String CB_CANCEL_FLOW = "FLOW:CANCEL";
    private static final String CB_SEARCH_PRODUCTS = "SEARCH:PRODUCTS";
    private static final String CB_SEARCH_RESERVE = "SEARCH:RESERVE";
    private static final String CB_SEARCH_UNRESERVE = "SEARCH:UNRESERVE";
    private static final String CB_SEARCH_SOLD = "SEARCH:SOLD";
    private static final String CB_SEARCH_RESERVE_ID = "SEARCH:RESERVE_ID";
    private static final String CB_SEARCH_SOLD_ID = "SEARCH:SOLD_ID";
    private static final String CB_SEARCH_MANUAL = "SEARCH:MANUAL";
    private static final String CB_SEARCH_EDIT = "SEARCH:EDIT";
    private static final String CB_SEARCH_EDIT_ID = "SEARCH:EDIT_ID";
    private static final String CB_DRAFT_EDIT = "DRAFT:EDIT";
    private static final String CB_DRAFT_READY = "DRAFT:READY";
    private static final String CB_DRAFT_FIELD_BRAND = "DRAFT:FIELD:BRAND";
    private static final String CB_DRAFT_FIELD_SIZE = "DRAFT:FIELD:SIZE";
    private static final String CB_DRAFT_FIELD_PRICE = "DRAFT:FIELD:PRICE";
    private static final String CB_DRAFT_FIELD_ARTICLE = "DRAFT:FIELD:ARTICLE";
    private static final String CB_DRAFT_PUBLISH_NOW = "DRAFT:PUBLISH_NOW";
    private static final String CB_DRAFT_PUBLISH_TIME = "DRAFT:PUBLISH_TIME";
    private static final String META_DISCOUNT_ENABLED = "discount:enabled";
    private static final String META_DISCOUNT_RESET_START = "discount:reset_start_date";
    private static final String META_ARTICLE_ENABLED = "article:enabled";

    private final Config config;
    private final Database db;
    private final ShopifyClient shopify;
    private final ShopifyCollections collections;
    private final KieAiClient kie;
    private String shopCurrency;
    private ZoneId discountZone;
    private final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter BELGRADE_DT_INPUT = DateTimeFormatter.ofPattern("dd.MM, HH:mm");
    private static final DateTimeFormatter BELGRADE_DT_SHOW = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm");

    private final ExecutorService workers = Executors.newFixedThreadPool(2);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean discountSyncRunning = new AtomicBoolean(false);
    private final AtomicBoolean discountSyncRetryScheduled = new AtomicBoolean(false);
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
        scheduler.scheduleWithFixedDelay(this::processScheduledPostsSafe, 20, 20, TimeUnit.SECONDS);
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
            if (!db.hasMetaKey(META_ARTICLE_ENABLED)) {
                db.setMeta(META_ARTICLE_ENABLED, "false");
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
            sendAddProductModeMenu(chatId);
            answerCallback(callback, "");
            return;
        }
        if (CB_ADD_PRODUCT_OLD.equals(data)) {
            resetSession(session);
            session.state = AdminState.ADD_PRODUCT_PHOTOS;
            sendPhotoUploadPrompt(chatId, session.pendingPhotoFileIds.size());
            answerCallback(callback, "");
            return;
        }
        if (CB_ADD_PRODUCT_AI.equals(data)) {
            resetSession(session);
            session.state = AdminState.AI_DRAFT_PHOTOS;
            sendAiPhotoPrompt(chatId, session, false);
            answerCallback(callback, "");
            return;
        }
        if (CB_WITHOUT_PHOTO.equals(data)) {
            resetSession(session);
            session.state = AdminState.WITHOUT_PHOTO_PHOTOS;
            sendAiPhotoPrompt(chatId, session, true);
            answerCallback(callback, "");
            return;
        }
        if (CB_SCHEDULED_POSTS.equals(data)) {
            resetSession(session);
            sendScheduledPostsPage(chatId, 0);
            answerCallback(callback, "");
            return;
        }
        if (CB_SCHEDULE_PLAN.equals(data)) {
            resetSession(session);
            sendPublicationSlotsPage(chatId, 0, null);
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
        if (CB_EDIT_POST.equals(data)) {
            resetSession(session);
            session.state = AdminState.EDIT_SELECT;
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
            db.setMeta("discount:last_sync_date", "");
            triggerDiscountSyncNow("enabled-by-admin");
            sendDiscountsDashboard(chatId, "▶️ Прогрессивные скидки включены.");
            answerCallback(callback, "Скидки включены");
            return;
        }
        if (CB_DISCOUNTS_RESET.equals(data)) {
            LocalDate currentStart = getDiscountResetStartDate();
            String startLabel = currentStart == null ? "не задана" : currentStart.toString();
            sendText(chatId,
                    "Подтвердите сброс цикла скидок.\n" +
                            "Текущая дата старта цикла: " + startLabel + "\n" +
                            "После подтверждения бот запишет сегодняшнюю дату и начнет цикл заново с дня 1.",
                    inlineSingleColumn(
                            button("✅ Подтвердить сброс", CB_DISCOUNTS_RESET_CONFIRM),
                            button("⬅ Назад к скидкам", CB_DISCOUNTS)
                    ));
            answerCallback(callback, "Нужно подтверждение");
            return;
        }
        if (CB_DISCOUNTS_RESET_CONFIRM.equals(data)) {
            String today = todayInDiscountZone().toString();
            db.setMeta(META_DISCOUNT_RESET_START, today);
            db.setMeta("discount:last_sync_date", "");
            log.warn("Discount cycle reset by admin {}. New start date={}", user.getId(), today);
            triggerDiscountSyncNow("reset-by-admin");
            sendDiscountsDashboard(chatId, "🔁 Цикл скидок сброшен.\nНовая дата старта цикла: " + today + "\nПересчет скидок запущен.");
            answerCallback(callback, "Цикл сброшен");
            return;
        }
        if (CB_DISCOUNTS_SET_DATE.equals(data)) {
            session.state = AdminState.DISCOUNT_CYCLE_DATE_INPUT;
            LocalDate currentStart = getDiscountResetStartDate();
            String startLabel = currentStart == null ? "не задана" : currentStart.toString();
            sendText(chatId,
                    "Введите дату старта цикла скидок.\n" +
                            "Текущая дата: " + startLabel + "\n" +
                            "Формат: `2026-06-01` или `01.06.2026`",
                    inlineSingleColumn(
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            answerCallback(callback, "");
            return;
        }
        if (CB_MANUAL_DISCOUNT.equals(data)) {
            resetSession(session);
            session.state = AdminState.MANUAL_DISCOUNT_SELECT;
            sendSelectableProductsPage(chatId, session, 0);
            answerCallback(callback, "");
            return;
        }
        if (CB_DRAFT_EDIT.equals(data)) {
            if (session.draft == null) {
                sendWelcomeMenu(chatId, "⚠️ Черновик не найден. Начните заново.");
                answerCallback(callback, "");
                return;
            }
            session.state = AdminState.DRAFT_EDIT_CHOICE;
            List<InlineKeyboardButton> editButtons = new ArrayList<>();
            editButtons.add(button("Бренд", CB_DRAFT_FIELD_BRAND));
            editButtons.add(button("Размер", CB_DRAFT_FIELD_SIZE));
            editButtons.add(button("Цена", CB_DRAFT_FIELD_PRICE));
            if (isArticleEnabled()) {
                editButtons.add(button("Артикль", CB_DRAFT_FIELD_ARTICLE));
            }
            editButtons.add(button("Отменить", CB_CANCEL_FLOW));
            sendText(chatId,
                    "Что редактировать?",
                    inlineSingleColumn(editButtons.toArray(new InlineKeyboardButton[0])));
            answerCallback(callback, "");
            return;
        }
        if (CB_DRAFT_READY.equals(data)) {
            if (session.draft == null) {
                sendWelcomeMenu(chatId, "⚠️ Черновик не найден. Начните заново.");
                answerCallback(callback, "");
                return;
            }
            session.state = AdminState.DRAFT_PUBLISH_CHOICE;
            sendText(chatId,
                    "Когда опубликовать?",
                    inlineSingleColumn(
                            button("Сейчас", CB_DRAFT_PUBLISH_NOW),
                            button("Запланированная публикация", CB_DRAFT_PUBLISH_TIME),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            answerCallback(callback, "");
            return;
        }
        if (CB_DRAFT_PUBLISH_NOW.equals(data)) {
            if (session.draft == null) {
                sendWelcomeMenu(chatId, "⚠️ Черновик не найден. Начните заново.");
                answerCallback(callback, "");
                return;
            }
            publishDraftNow(chatId, session);
            answerCallback(callback, "");
            return;
        }
        if (CB_DRAFT_PUBLISH_TIME.equals(data)) {
            if (session.draft == null) {
                sendWelcomeMenu(chatId, "⚠️ Черновик не найден. Начните заново.");
                answerCallback(callback, "");
                return;
            }
            offerDraftScheduleBySlots(chatId, session);
            answerCallback(callback, "");
            return;
        }
        if (CB_SLOT_ADD.equals(data)) {
            session.state = AdminState.SCHEDULE_SLOT_INPUT;
            sendText(chatId,
                    "Введите дату и время публикации по Белграду в формате: 22.05, 20:00",
                    inlineSingleColumn(button("Отменить", CB_CANCEL_FLOW)));
            answerCallback(callback, "");
            return;
        }
        if (data.startsWith("SLOT:DEL:")) {
            long slotId = 0;
            try {
                slotId = Long.parseLong(data.substring("SLOT:DEL:".length()));
            } catch (Exception ignored) {
            }
            if (slotId > 0) {
                db.deletePublicationSlot(slotId);
            }
            sendPublicationSlotsPage(chatId, 0, "🗑 Слот удален.");
            answerCallback(callback, "");
            return;
        }
        if (data.startsWith("DRAFT:SLOT:")) {
            long slotId = 0;
            try {
                slotId = Long.parseLong(data.substring("DRAFT:SLOT:".length()));
            } catch (Exception ignored) {
            }
            if (slotId <= 0) {
                answerCallback(callback, "Некорректный слот");
                return;
            }
            scheduleDraftWithSlot(chatId, session, slotId);
            answerCallback(callback, "");
            return;
        }
        if (CB_DRAFT_FIELD_BRAND.equals(data)) {
            session.state = AdminState.DRAFT_EDIT_BRAND_INPUT;
            sendText(chatId, "Введите новый бренд:", inlineSingleColumn(button("Отменить", CB_CANCEL_FLOW)));
            answerCallback(callback, "");
            return;
        }
        if (CB_DRAFT_FIELD_SIZE.equals(data)) {
            session.state = AdminState.DRAFT_EDIT_SIZE_INPUT;
            sendText(chatId, "Введите новый размер (например: muški L или ženski S):", inlineSingleColumn(button("Отменить", CB_CANCEL_FLOW)));
            answerCallback(callback, "");
            return;
        }
        if (CB_DRAFT_FIELD_PRICE.equals(data)) {
            session.state = AdminState.DRAFT_EDIT_PRICE_INPUT;
            sendText(chatId, "Введите новую цену в RSD:", inlineSingleColumn(button("Отменить", CB_CANCEL_FLOW)));
            answerCallback(callback, "");
            return;
        }
        if (CB_DRAFT_FIELD_ARTICLE.equals(data)) {
            if (!isArticleEnabled()) {
                answerCallback(callback, "Артикулы сейчас отключены");
                return;
            }
            session.state = AdminState.DRAFT_EDIT_ARTICLE_INPUT;
            sendText(chatId, "Введите новый артикул (8 цифр):", inlineSingleColumn(button("Отменить", CB_CANCEL_FLOW)));
            answerCallback(callback, "");
            return;
        }
        if (CB_DONE_PHOTOS.equals(data)) {
            if (session.state != AdminState.ADD_PRODUCT_PHOTOS
                    && session.state != AdminState.AI_DRAFT_PHOTOS
                    && session.state != AdminState.WITHOUT_PHOTO_PHOTOS) {
                session.state = AdminState.ADD_PRODUCT_PHOTOS;
            }
            if (session.state == AdminState.AI_DRAFT_PHOTOS || session.state == AdminState.WITHOUT_PHOTO_PHOTOS) {
                if (session.aiPhotoGroups.isEmpty()) {
                    sendAiPhotoPrompt(chatId, session, session.state == AdminState.WITHOUT_PHOTO_PHOTOS);
                    answerCallback(callback, "Сначала добавьте хотя бы одну группу фото");
                    return;
                }
                if (session.aiProcessing) {
                    answerCallback(callback, "Уже обрабатываю фото, подождите");
                    return;
                }
                buildDraftFromAi(chatId, session, session.state == AdminState.WITHOUT_PHOTO_PHOTOS);
                answerCallback(callback, "");
                return;
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
            if (session.state == AdminState.AI_DRAFT_PHOTOS || session.state == AdminState.WITHOUT_PHOTO_PHOTOS) {
                sendAiPhotoPrompt(chatId, session, session.state == AdminState.WITHOUT_PHOTO_PHOTOS);
            } else {
                session.state = AdminState.ADD_PRODUCT_PHOTOS;
                sendPhotoUploadPrompt(chatId, session.pendingPhotoFileIds.size());
            }
            answerCallback(callback, "");
            return;
        }
        if (CB_SEARCH_PRODUCTS.equals(data)) {
            session.state = AdminState.ARTICLE_SEARCH_INPUT;
            session.searchScope = ProductSearchScope.PRODUCTS;
            sendArticleSearchPrompt(chatId);
            answerCallback(callback, "");
            return;
        }
        if (CB_SEARCH_RESERVE.equals(data)) {
            session.state = AdminState.ARTICLE_SEARCH_INPUT;
            session.searchScope = ProductSearchScope.RESERVE;
            sendArticleSearchPrompt(chatId);
            answerCallback(callback, "");
            return;
        }
        if (CB_SEARCH_UNRESERVE.equals(data)) {
            session.state = AdminState.ARTICLE_SEARCH_INPUT;
            session.searchScope = ProductSearchScope.UNRESERVE;
            sendArticleSearchPrompt(chatId);
            answerCallback(callback, "");
            return;
        }
        if (CB_SEARCH_SOLD.equals(data)) {
            session.state = AdminState.ARTICLE_SEARCH_INPUT;
            session.searchScope = ProductSearchScope.SOLD;
            sendArticleSearchPrompt(chatId);
            answerCallback(callback, "");
            return;
        }
        if (CB_SEARCH_RESERVE_ID.equals(data)) {
            session.state = AdminState.PRODUCT_ID_SEARCH_INPUT;
            session.searchScope = ProductSearchScope.RESERVE;
            sendProductIdSearchPrompt(chatId, "резерва");
            answerCallback(callback, "");
            return;
        }
        if (CB_SEARCH_SOLD_ID.equals(data)) {
            session.state = AdminState.PRODUCT_ID_SEARCH_INPUT;
            session.searchScope = ProductSearchScope.SOLD;
            sendProductIdSearchPrompt(chatId, "продажи");
            answerCallback(callback, "");
            return;
        }
        if (CB_SEARCH_MANUAL.equals(data)) {
            session.state = AdminState.ARTICLE_SEARCH_INPUT;
            session.searchScope = ProductSearchScope.MANUAL;
            sendArticleSearchPrompt(chatId);
            answerCallback(callback, "");
            return;
        }
        if (CB_SEARCH_EDIT.equals(data)) {
            session.state = AdminState.ARTICLE_SEARCH_INPUT;
            session.searchScope = ProductSearchScope.EDIT;
            sendArticleSearchPrompt(chatId);
            answerCallback(callback, "");
            return;
        }
        if (CB_SEARCH_EDIT_ID.equals(data)) {
            session.state = AdminState.PRODUCT_ID_SEARCH_INPUT;
            session.searchScope = ProductSearchScope.EDIT;
            sendEditIdSearchPrompt(chatId);
            answerCallback(callback, "");
            return;
        }
        if (data.startsWith("PAGE:SCHEDULED:")) {
            int page;
            try {
                page = Integer.parseInt(data.substring("PAGE:SCHEDULED:".length()));
            } catch (NumberFormatException e) {
                page = 0;
            }
            sendScheduledPostsPage(chatId, page);
            answerCallback(callback, "");
            return;
        }
        if (data.startsWith("PAGE:SLOTS:")) {
            int page;
            try {
                page = Integer.parseInt(data.substring("PAGE:SLOTS:".length()));
            } catch (NumberFormatException e) {
                page = 0;
            }
            sendPublicationSlotsPage(chatId, page, null);
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
                case "EDIT":
                    session.state = AdminState.EDIT_SELECT;
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
        if (text.toLowerCase(Locale.ROOT).startsWith("/articul")) {
            handleArticulCommand(chatId, text);
            return;
        }
        if ("/cancel".equalsIgnoreCase(text)) {
            resetSession(session);
            sendWelcomeMenu(chatId);
            return;
        }

        if (session.state == AdminState.ADD_PRODUCT_PHOTOS
                || session.state == AdminState.AI_DRAFT_PHOTOS
                || session.state == AdminState.WITHOUT_PHOTO_PHOTOS) {
            boolean withoutPhoto = session.state == AdminState.WITHOUT_PHOTO_PHOTOS;
            boolean aiBatchMode = session.state == AdminState.AI_DRAFT_PHOTOS || session.state == AdminState.WITHOUT_PHOTO_PHOTOS;
            if (aiBatchMode && session.aiProcessing) {
                sendText(chatId, "⏳ Идет обработка групп через ИИ, дождитесь результата.");
                return;
            }
            if (message.getPhoto() != null && !message.getPhoto().isEmpty()) {
                if (!aiBatchMode && session.pendingPhotoFileIds.size() >= 9) {
                    sendPhotoUploadPrompt(chatId, session.pendingPhotoFileIds.size());
                    return;
                }
                if (aiBatchMode) {
                    int groupSize = addPhotoToAiBatch(session, message);
                    String mgid = message.getMediaGroupId();
                    // For albums, reply only once per media group (on first item) to avoid spam.
                    if (mgid == null || mgid.isBlank() || groupSize <= 1) {
                        sendAiPhotoPrompt(chatId, session, withoutPhoto);
                    }
                } else {
                    PhotoSize best = selectBestPhoto(message.getPhoto());
                    session.pendingPhotoFileIds.add(best.getFileId());
                    sendPhotoUploadPrompt(chatId, session.pendingPhotoFileIds.size());
                }
            } else {
                if (session.state == AdminState.ADD_PRODUCT_PHOTOS) {
                    sendPhotoUploadPrompt(chatId, session.pendingPhotoFileIds.size());
                } else {
                    sendAiPhotoPrompt(chatId, session, withoutPhoto);
                }
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
            DraftData draft = buildDraftFromManualDescription(chatId, session, text);
            if (draft == null) {
                return;
            }
            session.draft = draft;
            session.state = AdminState.DRAFT_PUBLISH_CHOICE;
            sendText(chatId,
                    "Когда опубликовать?",
                    inlineSingleColumn(
                            button("Сейчас", CB_DRAFT_PUBLISH_NOW),
                            button("Запланированная публикация", CB_DRAFT_PUBLISH_TIME),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }

        if (session.state == AdminState.EDIT_DESCRIPTION_INPUT) {
            if (session.selectedProductId <= 0) {
                resetSession(session);
                sendWelcomeMenu(chatId, "⚠️ Товар для редактирования не выбран. Начните заново.");
                return;
            }
            if (text.isBlank()) {
                sendText(chatId,
                        "✍️ Описание пустое. Отправьте полный новый текст.",
                        inlineSingleColumn(
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
                return;
            }
            editProductFromAdminInput(chatId, session, text);
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

        if (session.state == AdminState.ARTICLE_SEARCH_INPUT) {
            String article = text.replaceAll("\\D", "");
            if (!article.matches("\\d{8}")) {
                sendText(chatId,
                        "Введите артикул из 8 цифр (например: 56789356).",
                        inlineSingleColumn(
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
                return;
            }
            processArticleSearch(chatId, session, article);
            return;
        }

        if (session.state == AdminState.PRODUCT_ID_SEARCH_INPUT) {
            long productId;
            try {
                productId = Long.parseLong(text.trim());
            } catch (NumberFormatException e) {
                sendText(chatId,
                        "Введите числовой ID товара из Shopify (например: 15086220312948).",
                        inlineSingleColumn(
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
                return;
            }
            processSearchByProductId(chatId, session, productId);
            return;
        }

        if (session.state == AdminState.RESERVE_SELECT
                || session.state == AdminState.UNRESERVE_SELECT
                || session.state == AdminState.SOLD_SELECT
                || session.state == AdminState.MANUAL_DISCOUNT_SELECT
                || session.state == AdminState.EDIT_SELECT) {
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

        if (session.state == AdminState.DRAFT_EDIT_BRAND_INPUT
                || session.state == AdminState.DRAFT_EDIT_SIZE_INPUT
                || session.state == AdminState.DRAFT_EDIT_PRICE_INPUT
                || session.state == AdminState.DRAFT_EDIT_ARTICLE_INPUT) {
            applyDraftFieldEdit(chatId, session, text);
            return;
        }

        if (session.state == AdminState.SCHEDULE_SLOT_INPUT) {
            addPublicationSlotFromInput(chatId, session, text);
            return;
        }

        if (session.state == AdminState.DISCOUNT_CYCLE_DATE_INPUT) {
            applyDiscountCycleStartDate(chatId, session, text);
            return;
        }

        sendWelcomeMenu(chatId, "Используйте кнопки ниже.");
    }

    private void createProductFromAdminInput(long chatId, AdminSession session, String rawDescription) {
        boolean articleEnabled = isArticleEnabled();
        String article = TextParser.extractArticle(rawDescription);
        if (articleEnabled) {
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
        } else {
            article = generateInternalArticle();
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
        String size = normalizeSizeWithGender(TextParser.extractSize(rawDescription), rawDescription);

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
        final String finalArticle = article;
        final boolean finalArticleEnabled = articleEnabled;
        final double finalBasePrice = basePrice;
        final String finalRawDescription = rawDescription;
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
            String captionCore = buildProductCaption(finalTitle, finalSize, finalBasePrice, finalBasePrice, finalArticle, 0, null, "ACTIVE");
            String telegramCaption = buildTelegramPostCaption(captionCore);

            long productId = 0;
            PublishResult pub = null;
            try {
                if (finalArticleEnabled) {
                    ProductCard existing = db.findProductCardByArticle(finalArticle);
                    if (existing != null) {
                        sendWelcomeMenu(chatId,
                                "⚠️ Артикул " + finalArticle + " уже существует (" + statusLabel(existing.status) + ").\n" +
                                        "Выберите другой артикул.");
                        return;
                    }
                }

                ShopifyClient.ProductPayload payload = new ShopifyClient.ProductPayload();
                payload.title = finalTitle;
                payload.bodyHtml = captionCore.replace("\n", "<br>");
                payload.priceEur = formatPriceForShopify(finalBasePrice);
                payload.size = finalSize;
                payload.tags = buildTags(explicit, new Classification());
                payload.productType = selectProductType(explicit);
                payload.images = images;
                if (finalArticleEnabled) {
                    payload.barcode = finalArticle;
                    payload.sku = finalArticle;
                }

                productId = shopify.createProduct(payload);
                ensureShopifyBodyHasProductId(productId, payload.title, payload.bodyHtml, payload.priceEur, payload.size, payload.barcode);
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

                pub = publishToListingChat(telegramCaption, images);
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
                        finalRawDescription,
                        finalArticle,
                        finalBasePrice,
                        finalBasePrice,
                        0,
                        null,
                        "ACTIVE",
                        Instant.now().getEpochSecond(),
                        Instant.now().getEpochSecond()
                ));

                String publishedText = finalArticleEnabled
                        ? ("✅ Товар опубликован.\nНазвание: " + finalTitle + "\nЦена: " + formatRsd(finalBasePrice) + " RSD\nАртикул: " + finalArticle)
                        : ("✅ Товар опубликован.\nНазвание: " + finalTitle + "\nЦена: " + formatRsd(finalBasePrice) + " RSD");
                sendWelcomeMenu(chatId, publishedText);
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

    private DraftData buildDraftFromManualDescription(long chatId, AdminSession session, String rawDescription) {
        boolean articleEnabled = isArticleEnabled();
        String article = TextParser.extractArticle(rawDescription);
        if (articleEnabled) {
            if (article == null || !article.matches("\\d{8}")) {
                sendText(chatId,
                        "❗ Не найден корректный артикул (8 цифр).\nПример: Artikal: 56789356",
                        inlineSingleColumn(
                                button("Назад", CB_BACK_TO_PHOTOS),
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
                return null;
            }
            ProductCard existingByArticle = db.findProductCardByArticle(article);
            if (existingByArticle != null && ("ACTIVE".equals(existingByArticle.status) || "RESERVED".equals(existingByArticle.status) || "POS_ONLY".equals(existingByArticle.status))) {
                sendText(chatId,
                        "⚠️ Этот артикул уже используется.\n" +
                                "Товар: " + existingByArticle.title + "\n" +
                                "Статус: " + statusLabel(existingByArticle.status) + "\n" +
                                "Укажите другой артикул.",
                        inlineSingleColumn(
                                button("Назад", CB_BACK_TO_PHOTOS),
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
                return null;
            }
        }

        String rsd = TextParser.extractRsd(rawDescription);
        if (rsd == null || rsd.isBlank()) {
            sendText(chatId,
                    "❗ Не удалось прочитать цену.\nУкажите строку вида: Cena - 1500 RSD",
                    inlineSingleColumn(
                            button("Назад", CB_BACK_TO_PHOTOS),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return null;
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
            return null;
        }
        if (basePrice <= 0) {
            sendText(chatId,
                    "❗ Цена должна быть больше 0.",
                    inlineSingleColumn(
                            button("Назад", CB_BACK_TO_PHOTOS),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return null;
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
            return null;
        }
        String size = normalizeSizeWithGender(TextParser.extractSize(rawDescription), rawDescription);

        if (session.pendingPhotoFileIds.isEmpty()) {
            sendPhotoUploadPrompt(chatId, 0);
            return null;
        }

        DraftData draft = new DraftData();
        draft.mode = DraftMode.ONLINE_WITH_PHOTO;
        draft.photoFileIds = new ArrayList<>(session.pendingPhotoFileIds);
        draft.title = title;
        draft.size = size;
        draft.priceRsd = basePrice;
        draft.article = articleEnabled ? article : "";
        draft.status = "ACTIVE";
        draft.rawDescription = rawDescription;
        return draft;
    }

    private void editProductFromAdminInput(long chatId, AdminSession session, String rawDescription) {
        ProductCard current = db.findProductCardById(session.selectedProductId);
        if (current == null) {
            resetSession(session);
            sendWelcomeMenu(chatId, "⚠️ Товар для редактирования не найден.");
            return;
        }

        boolean articleEnabled = isArticleEnabled();
        String article = TextParser.extractArticle(rawDescription);
        if (articleEnabled) {
            if (article == null || !article.matches("\\d{8}")) {
                sendText(chatId,
                        "❗ Не найден корректный артикул (8 цифр).\nПример: Artikal: 56789356",
                        inlineSingleColumn(
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
                return;
            }
            ProductCard existingByArticle = db.findProductCardByArticle(article);
            if (existingByArticle != null && existingByArticle.productId != current.productId) {
                sendText(chatId,
                        "⚠️ Этот артикул уже используется.\n" +
                                "Товар: " + existingByArticle.title + "\n" +
                                "Статус: " + statusLabel(existingByArticle.status) + "\n" +
                                "Укажите другой артикул.",
                        inlineSingleColumn(
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
                return;
            }
        } else {
            article = current.article == null || current.article.isBlank() ? generateInternalArticle() : current.article;
        }

        String rsd = TextParser.extractRsd(rawDescription);
        if (rsd == null || rsd.isBlank()) {
            sendText(chatId,
                    "❗ Не удалось прочитать цену.\nУкажите строку вида: Cena - 1500 RSD",
                    inlineSingleColumn(
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
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }
        if (basePrice <= 0) {
            sendText(chatId,
                    "❗ Цена должна быть больше 0.",
                    inlineSingleColumn(
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
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }
        String size = normalizeSizeWithGender(TextParser.extractSize(rawDescription), rawDescription);
        String statusForCaption = "RESERVED".equals(current.status) ? "RESERVED" : "ACTIVE";

        final String finalTitle = title;
        final String finalSize = size;
        final String finalArticle = article;
        final double finalBasePrice = basePrice;
        final long oldProductId = current.productId;
        final String postChannelId = current.channelId;
        final long postMessageId = current.messageId;
        final String postMediaGroupId = current.mediaGroupId;
        final long oldCreatedAt = current.createdAt;

        sendText(chatId, "⏳ Обновляю товар, подождите 3-10 секунд...");
        workers.submit(() -> {
            long newProductId = 0;
            try {
                List<String> imageUrls = shopify.getProductImageUrls(oldProductId);
                if (imageUrls.isEmpty()) {
                    sendWelcomeMenu(chatId, "❗ У товара нет изображений в Shopify, редактирование невозможно.");
                    return;
                }

                CategorySelection explicit = TextParser.detectExplicitCategories(rawDescription);
                if (explicit.entries.isEmpty()) {
                    explicit.add("Muško", null);
                }
                String captionCore = buildProductCaption(finalTitle, finalSize, finalBasePrice, finalBasePrice, finalArticle, 0, null, statusForCaption);
                String telegramCaption = buildTelegramPostCaption(captionCore);

                ShopifyClient.ProductPayload payload = new ShopifyClient.ProductPayload();
                payload.title = "RESERVED".equals(statusForCaption) ? ("REZERVISANO | " + finalTitle) : finalTitle;
                payload.bodyHtml = captionCore.replace("\n", "<br>");
                payload.priceEur = formatPriceForShopify(finalBasePrice);
                payload.size = finalSize;
                payload.tags = buildTags(explicit, new Classification());
                payload.productType = selectProductType(explicit);
                payload.imageUrls = imageUrls;
                if (articleEnabled) {
                    payload.barcode = finalArticle;
                    payload.sku = finalArticle;
                }

                newProductId = shopify.createProduct(payload);
                ensureShopifyBodyHasProductId(newProductId, payload.title, payload.bodyHtml, payload.priceEur, payload.size, payload.barcode);
                if (config.shopifyPublishAll) {
                    try {
                        shopify.publishProductToAll(newProductId);
                    } catch (Exception e) {
                        log.warn("Failed to publish updated product {} to channels", newProductId, e);
                    }
                }
                for (CategorySelection.Entry entry : explicit.entries) {
                    for (String titleKey : collections.buildCollectionTitles(entry.section, entry.subcategory)) {
                        Long id = collections.getCollectionId(titleKey);
                        if (id != null) {
                            try {
                                shopify.addProductToCollection(newProductId, id);
                            } catch (Exception e) {
                                log.warn("Failed to add updated product {} to collection {}", newProductId, titleKey, e);
                            }
                        }
                    }
                }

                String telegramLink = buildTelegramLink(postChannelId, postMessageId);
                if (telegramLink != null && !telegramLink.isBlank()) {
                    try {
                        shopify.setProductMetafield(newProductId,
                                config.telegramLinkMetafieldNamespace,
                                config.telegramLinkMetafieldKey,
                                telegramLink,
                                config.telegramLinkMetafieldType);
                    } catch (Exception e) {
                        log.warn("Failed to set telegram link metafield for updated product {}", newProductId, e);
                    }
                }

                try {
                    shopify.deleteProduct(oldProductId);
                } catch (Exception e) {
                    log.warn("Failed to delete old product {} during edit", oldProductId, e);
                }

                try {
                    editTelegramCaption(postChannelId, postMessageId, telegramCaption);
                } catch (Exception e) {
                    log.warn("Failed to edit Telegram caption for updated product {}", newProductId, e);
                }
                db.updatePostText(postChannelId, postMessageId, telegramCaption);

                if (postMediaGroupId != null && !postMediaGroupId.isBlank()) {
                    db.setProductIdForMediaGroup(postChannelId, postMediaGroupId, newProductId);
                } else {
                    db.setProductIdForMessage(postChannelId, postMessageId, newProductId);
                }

                db.deleteProductCard(oldProductId);
                db.upsertProductCard(new ProductCard(
                        newProductId,
                        postChannelId,
                        postMessageId,
                        postMediaGroupId,
                        finalTitle,
                        finalSize,
                        rawDescription,
                        finalArticle,
                        finalBasePrice,
                        finalBasePrice,
                        0,
                        null,
                        statusForCaption,
                        oldCreatedAt > 0 ? oldCreatedAt : Instant.now().getEpochSecond(),
                        Instant.now().getEpochSecond()
                ));

                resetSession(session);
                String updatedText = articleEnabled
                        ? ("✅ Товар обновлен.\nНазвание: " + finalTitle + "\nЦена: " + formatRsd(finalBasePrice) + " RSD\nАртикул: " + finalArticle)
                        : ("✅ Товар обновлен.\nНазвание: " + finalTitle + "\nЦена: " + formatRsd(finalBasePrice) + " RSD");
                sendWelcomeMenu(chatId, updatedText);
            } catch (Exception e) {
                log.error("Failed to edit product from admin flow", e);
                if (newProductId > 0) {
                    try {
                        shopify.deleteProduct(newProductId);
                    } catch (Exception ignored) {
                    }
                }
                sendWelcomeMenu(chatId, "❌ Ошибка редактирования товара: " + e.getMessage());
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
            boolean articleEnabled = isArticleEnabled();
            for (int i = 0; i < items.size(); i++) {
                ProductCard card = items.get(i);
                sb.append("\n")
                        .append(startOrdinal + i)
                        .append(". ")
                        .append(card.title)
                        .append(" | ")
                        .append(formatRsd(card.currentPriceRsd))
                        .append(" RSD")
                        .append(" | ID: ")
                        .append(card.productId);
                if (articleEnabled && card.article != null && !card.article.isBlank()) {
                    sb.append(" | Artikal: ").append(card.article);
                } else if (card.size != null && !card.size.isBlank()) {
                    sb.append(" | Vel - ").append(card.size);
                }
                sb
                        .append(" | ")
                        .append("RESERVED".equals(card.status) ? "REZERVISANO" : "AKTIVAN");
            }
        }
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        appendPaginationRows(rows, "PRODUCTS", safePage, pages);
        if (isArticleEnabled()) {
            rows.add(List.of(button("🔎 Поиск по артиклу", CB_SEARCH_PRODUCTS)));
        }
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
        } else if (session.state == AdminState.EDIT_SELECT) {
            sb.append("✏️ Выберите товар для редактирования.");
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
            boolean articleEnabled = isArticleEnabled();
            for (int i = 0; i < items.size(); i++) {
                ProductCard card = items.get(i);
                sb.append("\n")
                        .append(startOrdinal + i)
                        .append(". ")
                        .append(card.title)
                        .append(" | ")
                        .append(formatRsd(card.currentPriceRsd))
                        .append(" RSD")
                        .append(" | ID: ")
                        .append(card.productId);
                if (articleEnabled && card.article != null && !card.article.isBlank()) {
                    sb.append(" | Artikal: ").append(card.article);
                } else if (card.size != null && !card.size.isBlank()) {
                    sb.append(" | Vel - ").append(card.size);
                }
            }
        }

        String scope = session.state == AdminState.RESERVE_SELECT ? "RESERVE"
                : session.state == AdminState.UNRESERVE_SELECT ? "UNRESERVE"
                : session.state == AdminState.MANUAL_DISCOUNT_SELECT ? "MANUAL"
                : session.state == AdminState.EDIT_SELECT ? "EDIT"
                : "SOLD";
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        appendPaginationRows(rows, scope, safePage, pages);
        if ("EDIT".equals(scope)) {
            if (isArticleEnabled()) {
                rows.add(List.of(
                        button("🔎 Поиск по артиклу", CB_SEARCH_EDIT),
                        button("🆔 Поиск по ID", CB_SEARCH_EDIT_ID)
                ));
            } else {
                rows.add(List.of(button("🆔 Поиск по ID", CB_SEARCH_EDIT_ID)));
            }
        } else if ("RESERVE".equals(scope)) {
            if (isArticleEnabled()) {
                rows.add(List.of(
                        button("🔎 Поиск по артиклу", CB_SEARCH_RESERVE),
                        button("🆔 Поиск по ID", CB_SEARCH_RESERVE_ID)
                ));
            } else {
                rows.add(List.of(button("🆔 Поиск по ID", CB_SEARCH_RESERVE_ID)));
            }
        } else if ("SOLD".equals(scope)) {
            if (isArticleEnabled()) {
                rows.add(List.of(
                        button("🔎 Поиск по артиклу", CB_SEARCH_SOLD),
                        button("🆔 Поиск по ID", CB_SEARCH_SOLD_ID)
                ));
            } else {
                rows.add(List.of(button("🆔 Поиск по ID", CB_SEARCH_SOLD_ID)));
            }
        } else if (isArticleEnabled()) {
            rows.add(List.of(button("🔎 Поиск по артиклу", searchCallbackForScope(scope))));
        }
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

    private String searchCallbackForScope(String scope) {
        if ("RESERVE".equals(scope)) return CB_SEARCH_RESERVE;
        if ("UNRESERVE".equals(scope)) return CB_SEARCH_UNRESERVE;
        if ("MANUAL".equals(scope)) return CB_SEARCH_MANUAL;
        if ("SOLD".equals(scope)) return CB_SEARCH_SOLD;
        if ("EDIT".equals(scope)) return CB_SEARCH_EDIT;
        return CB_SEARCH_PRODUCTS;
    }

    private void sendArticleSearchPrompt(long chatId) {
        if (!isArticleEnabled()) {
            sendText(chatId, "Поиск по артиклу недоступен, пока артикулы отключены (/articul on).");
            return;
        }
        sendText(chatId,
                "🔎 Введите артикул (8 цифр), например: 56789356",
                inlineSingleColumn(
                        button("Отменить", CB_CANCEL_FLOW)
                ));
    }

    private void sendEditIdSearchPrompt(long chatId) {
        sendText(chatId,
                "🆔 Введите ID товара из Shopify.\nПример: 15086220312948",
                inlineSingleColumn(
                        button("Отменить", CB_CANCEL_FLOW)
                ));
    }

    private void sendProductIdSearchPrompt(long chatId, String actionLabel) {
        sendText(chatId,
                "🆔 Введите ID товара из Shopify для " + actionLabel + ".\nПример: 15086220312948",
                inlineSingleColumn(
                        button("Отменить", CB_CANCEL_FLOW)
                ));
    }

    private void processArticleSearch(long chatId, AdminSession session, String article) {
        if (!isArticleEnabled()) {
            sendText(chatId, "Поиск по артиклу недоступен, пока артикулы отключены (/articul on).");
            return;
        }
        ProductSearchScope scope = session.searchScope == null ? ProductSearchScope.PRODUCTS : session.searchScope;
        Database.ProductCard card;
        Integer ordinal;
        switch (scope) {
            case UNRESERVE:
                card = db.findReservedProductByArticle(article);
                if (card == null) {
                    sendText(chatId, "По этому артикулу нет товара в резерве.");
                    return;
                }
                ordinal = db.findReservedOrdinalByProductId(card.productId);
                session.state = AdminState.UNRESERVE_SELECT;
                break;
            case RESERVE:
                card = db.findVisibleProductByArticle(article);
                if (card == null) {
                    sendText(chatId, "Товар с таким артикулом не найден среди активных.");
                    return;
                }
                ordinal = db.findVisibleOrdinalByProductId(card.productId);
                session.state = AdminState.RESERVE_SELECT;
                break;
            case SOLD:
                card = db.findVisibleProductByArticle(article);
                if (card == null) {
                    sendText(chatId, "Товар с таким артикулом не найден среди активных.");
                    return;
                }
                ordinal = db.findVisibleOrdinalByProductId(card.productId);
                session.state = AdminState.SOLD_SELECT;
                break;
            case MANUAL:
                card = db.findVisibleProductByArticle(article);
                if (card == null) {
                    sendText(chatId, "Товар с таким артикулом не найден среди активных.");
                    return;
                }
                ordinal = db.findVisibleOrdinalByProductId(card.productId);
                session.state = AdminState.MANUAL_DISCOUNT_SELECT;
                break;
            case EDIT:
                card = db.findVisibleProductByArticle(article);
                if (card == null) {
                    sendText(chatId, "Товар с таким артикулом не найден среди активных.");
                    return;
                }
                ordinal = db.findVisibleOrdinalByProductId(card.productId);
                session.state = AdminState.EDIT_SELECT;
                break;
            case PRODUCTS:
            default:
                card = db.findVisibleProductByArticle(article);
                if (card == null) {
                    sendText(chatId, "Товар с таким артикулом не найден среди активных.");
                    return;
                }
                ordinal = db.findVisibleOrdinalByProductId(card.productId);
                break;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("✅ Найдено:\n");
        sb.append(card.title)
                .append(" | ")
                .append(formatRsd(card.currentPriceRsd))
                .append(" RSD\n");
        sb.append("ID: ").append(card.productId).append("\n");
        sb.append("Artikal: ").append(card.article).append("\n");
        sb.append("Статус: ").append(statusLabel(card.status));
        if (ordinal != null && ordinal > 0) {
            sb.append("\nНомер в списке: ").append(ordinal);
            if (scope != ProductSearchScope.PRODUCTS) {
                sb.append("\nВведите этот номер цифрой для действия.");
            }
        }
        sendText(chatId, sb.toString());
    }

    private void processSearchByProductId(long chatId, AdminSession session, long productId) {
        ProductSearchScope scope = session.searchScope == null ? ProductSearchScope.EDIT : session.searchScope;
        ProductCard card = db.findProductCardById(productId);
        if (card == null) {
            sendText(chatId, "Товар с таким ID не найден.");
            return;
        }

        if (scope == ProductSearchScope.EDIT) {
            if (!"ACTIVE".equals(card.status) && !"RESERVED".equals(card.status)) {
                sendText(chatId, "Товар с таким ID не найден среди активных/зарезервированных.");
                return;
            }
            Integer ordinal = db.findVisibleOrdinalByProductId(card.productId);
            StringBuilder sb = new StringBuilder();
            sb.append("✅ Найдено по ID:\n");
            sb.append(card.title).append(" | ").append(formatRsd(card.currentPriceRsd)).append(" RSD\n");
            sb.append("ID: ").append(card.productId);
            if (ordinal != null && ordinal > 0) {
                sb.append("\nНомер в списке: ").append(ordinal);
            }
            sendText(chatId, sb.toString());
            beginEditFlow(chatId, session, card);
            return;
        }

        if (scope == ProductSearchScope.RESERVE || scope == ProductSearchScope.SOLD) {
            if (!"ACTIVE".equals(card.status) && !"RESERVED".equals(card.status)) {
                sendText(chatId, "Товар с таким ID не найден среди активных/зарезервированных.");
                return;
            }
            Integer ordinal = db.findVisibleOrdinalByProductId(card.productId);
            if (ordinal == null || ordinal <= 0) {
                sendText(chatId, "Товар найден, но не отображается в текущем списке. Попробуйте снова.");
                return;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("✅ Найдено по ID:\n");
            sb.append(card.title).append(" | ").append(formatRsd(card.currentPriceRsd)).append(" RSD\n");
            sb.append("ID: ").append(card.productId).append("\n");
            sb.append("Номер в списке: ").append(ordinal).append("\n");
            sb.append("Введите этот номер цифрой для действия.");
            sendText(chatId, sb.toString());
            session.state = (scope == ProductSearchScope.RESERVE) ? AdminState.RESERVE_SELECT : AdminState.SOLD_SELECT;
            return;
        }

        sendText(chatId, "Поиск по ID для этого раздела пока не поддерживается.");
    }

    private void markCardAsSold(ProductCard card) throws IOException {
        markTelegramCardAsProdato(card);
        try {
            shopify.deleteProduct(card.productId);
        } catch (Exception e) {
            log.warn("Failed to delete product {} from Shopify", card.productId, e);
        }
        db.markProductStatus(card.productId, "SOLD");
        db.deleteProductCard(card.productId);
    }

    private boolean markTelegramCardAsProdato(ProductCard card) {
        if (card == null || "POS_ONLY".equals(card.status) || card.messageId <= 0) {
            return true;
        }
        try {
            editTelegramCaption(card.channelId, card.messageId, "PRODATO");
            db.updatePostText(card.channelId, card.messageId, "PRODATO");
            return true;
        } catch (Exception e) {
            if (isTelegramMessageMissingError(e)) {
                log.warn("Telegram message already missing for product {}, treating PRODATO caption as already gone", card.productId);
                return true;
            }
            log.warn("Failed to put PRODATO caption for product {}", card.productId, e);
            return false;
        }
    }

    private void syncCardState(ProductCard card, String status, int discountPercent, Double fixedPriceRsd, double currentPriceRsd) throws IOException {
        syncCardState(card, status, discountPercent, fixedPriceRsd, currentPriceRsd, true);
    }

    private void syncCardState(ProductCard card,
                               String status,
                               int discountPercent,
                               Double fixedPriceRsd,
                               double currentPriceRsd,
                               boolean syncSaleCollection) throws IOException {
        ShopifyProductSnapshot snapshot;
        try {
            snapshot = shopify.getProductSnapshot(card.productId);
        } catch (IOException e) {
            if (isShopifyProductMissingError(e)) {
                handleMissingShopifyProduct(card, "sync-card-state");
                return;
            }
            throw e;
        }
        String shopifyTitle = "RESERVED".equals(status) ? ("REZERVISANO | " + card.title) : card.title;
        String captionCore = buildProductCaption(card.title, card.size, card.basePriceRsd, currentPriceRsd, card.article, discountPercent, fixedPriceRsd, status);
        String telegramCaption = buildTelegramPostCaption(captionCore);
        String bodyHtml = withShopifyProductIdFooterHtml(captionCore.replace("\n", "<br>"), card.productId);

        shopify.updateProduct(
                card.productId,
                snapshot.variantId,
                shopifyTitle,
                bodyHtml,
                formatPriceForShopify(currentPriceRsd),
                card.size,
                card.article
        );
        if (syncSaleCollection) {
            syncSaleCollection(card.productId, discountPercent > 0 || fixedPriceRsd != null);
        }
        boolean shouldStoreCaption = true;
        try {
            editTelegramCaption(card.channelId, card.messageId, telegramCaption);
        } catch (TelegramApiException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
            if (!msg.contains("message is not modified")) {
                if (isTelegramMessageMissingError(e)) {
                    shouldStoreCaption = false;
                    log.warn("Telegram message missing for product {}, Shopify price updated; saving DB pricing without caption edit", card.productId);
                } else {
                    throw new IOException("Failed to edit Telegram caption", e);
                }
            }
        }
        if (shouldStoreCaption) {
            db.updatePostText(card.channelId, card.messageId, telegramCaption);
        }
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
            if (isAlreadySyncedSaleCollectionError(e)) {
                return;
            }
            log.warn("Failed to sync Sniženje collection for product {}", productId, e);
        }
    }

    private void handleMissingShopifyProduct(ProductCard card, String source) {
        if (card == null) {
            return;
        }
        if (!markTelegramCardAsProdato(card)) {
            log.warn("Product {} missing in Shopify during {}, but Telegram caption update failed. Will retry later.", card.productId, source);
            return;
        }
        db.markProductStatus(card.productId, "SOLD");
        db.deleteProductCard(card.productId);
        log.info("Product missing in Shopify during {}, marked as PRODATO and cleaned references. productId={}", source, card.productId);
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
        if (isArticleEnabled() && article != null && !article.isBlank()) {
            lines.add("Artikal: " + article);
        }
        return String.join("\n", lines);
    }

    private String buildTelegramPostCaption(String coreCaption) {
        if (coreCaption == null || coreCaption.isBlank()) {
            return ORDER_CONTACT_FOOTER;
        }
        return coreCaption + "\n\n" + ORDER_CONTACT_FOOTER;
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
        final int MAX_TG_TEXT = 3800;
        LocalDate today = LocalDate.now(discountZone == null ? ZoneId.systemDefault() : discountZone);
        boolean enabled = isDiscountsEnabled();
        String phase = describeDiscountStage(today);
        String dayInfo = describeDiscountDay(today);
        String dateLabel = today.format(DateTimeFormatter.ofPattern("dd.MM.yyyy"));
        LocalDate cycleStart = getDiscountResetStartDate();
        List<ProductCard> cards = db.listVisibleProducts(5000, 0);

        StringBuilder text = new StringBuilder();
        text.append("🧾 Скидки\n");
        text.append("Статус: ").append(enabled ? "включены ✅" : "отключены ⏸").append("\n");
        text.append("Сегодня: ").append(dateLabel).append("\n");
        text.append("Дата старта цикла: ").append(cycleStart == null ? "не задана" : cycleStart.toString()).append("\n");
        text.append("День цикла: ").append(dayInfo).append("\n");
        text.append("Текущий этап: ").append(phase).append("\n");
        text.append("Скидочный день недели: воскресенье.\n");
        text.append("\n");
        text.append("Автосистема применяет изменения 1 раз в день (до 09:00 Belgrade).\n");
        text.append("Прогрессия: каждое воскресенье 0% → 15% → 30% → 50%.\n");
        text.append("Последняя неделя месяца: Пн 20%, Вт 30%, Ср 40%, Чт 50%, Пт 500, Сб/Вс 350.");
        text.append("\n");
        text.append(buildDiscountGroupSummary(cards, today));
        text.append("\n\nТовары в работе: ").append(cards.size());
        if (cards.isEmpty()) {
            text.append("\nСписок пуст.");
        } else {
            boolean articleEnabled = isArticleEnabled();
            int shown = 0;
            int hidden = 0;
            for (int i = 0; i < cards.size(); i++) {
                ProductCard card = cards.get(i);
                DiscountTarget target = calculateDiscountTarget(card, today);
                int currentDiscount = card.fixedPriceRsd != null
                        ? discountPercentByFixed(card.basePriceRsd, card.fixedPriceRsd)
                        : card.discountPercent;
                StringBuilder item = new StringBuilder();
                item.append("\n")
                        .append(i + 1)
                        .append(". ")
                        .append(card.title)
                        .append(" | ")
                        .append(formatRsd(card.currentPriceRsd))
                        .append(" RSD");
                if (articleEnabled && card.article != null && !card.article.isBlank()) {
                    item.append(" | Artikal: ").append(card.article);
                } else if (card.size != null && !card.size.isBlank()) {
                    item.append(" | Vel - ").append(card.size);
                }
                item.append("\n   текущая скидка: ").append(currentDiscount).append("%");
                if (target != null) {
                    item.append(" | авто сегодня: ").append(target.discountPercent).append("%");
                    if (target.fixedPriceRsd != null) {
                        item.append(" (фикс ").append(formatRsd(target.fixedPriceRsd)).append(")");
                    } else {
                        item.append(" (").append(formatRsd(target.currentPriceRsd)).append(" RSD)");
                    }
                }
                if (text.length() + item.length() > MAX_TG_TEXT - 120) {
                    hidden = cards.size() - i;
                    break;
                }
                text.append(item);
                shown++;
            }
            if (hidden > 0) {
                text.append("\n… Показано ").append(shown).append(" из ").append(cards.size()).append(" товаров.");
                text.append("\nОткройте «Список товаров» для полного списка.");
            }
        }
        if (statusMessage != null && !statusMessage.isBlank()) {
            String suffix = "\n\n" + statusMessage;
            if (text.length() + suffix.length() <= MAX_TG_TEXT) {
                text.append(suffix);
            } else {
                int left = MAX_TG_TEXT - text.length() - 4;
                if (left > 20) {
                    text.append("\n\n").append(statusMessage, 0, Math.min(statusMessage.length(), left)).append("…");
                }
            }
        }

        InlineKeyboardButton toggle = enabled
                ? button("⏸ Отключить скидки", CB_DISCOUNTS_DISABLE)
                : button("▶️ Включить скидки", CB_DISCOUNTS_ENABLE);
        sendText(chatId, text.toString(), inlineRows(
                List.of(toggle),
                List.of(
                        button("📅 Установить дату цикла", CB_DISCOUNTS_SET_DATE),
                        button("🔁 Сбросить цикл", CB_DISCOUNTS_RESET)
                ),
                List.of(button("🏷 Скидка на товар", CB_MANUAL_DISCOUNT)),
                List.of(button("⬅ Назад в меню", CB_MENU))
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

    private Integer getDiscountCycleDay(LocalDate today) {
        LocalDate resetStart = getDiscountResetStartDate();
        if (resetStart == null || today == null || today.isBefore(resetStart)) {
            return null;
        }
        long days = java.time.temporal.ChronoUnit.DAYS.between(resetStart, today);
        return (int) (days % 28L) + 1;
    }

    private boolean isFinalDiscountWeek(LocalDate today) {
        Integer cycleDay = getDiscountCycleDay(today);
        if (cycleDay != null) {
            return cycleDay >= 22;
        }
        LocalDate monthEnd = YearMonth.from(today).atEndOfMonth();
        LocalDate lastWeekStart = monthEnd.with(java.time.temporal.TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY));
        return !today.isBefore(lastWeekStart);
    }

    private String describeDiscountStage(LocalDate today) {
        Integer cycleDay = getDiscountCycleDay(today);
        if (cycleDay != null) {
            if (cycleDay <= 6) return "1 неделя цикла: без скидок";
            if (cycleDay == 7) return "1 неделя цикла: воскресенье 15%";
            if (cycleDay <= 13) return "2 неделя цикла: ожидание воскресенья 30%";
            if (cycleDay == 14) return "2 неделя цикла: воскресенье 30%";
            if (cycleDay <= 20) return "3 неделя цикла: ожидание воскресенья 50%";
            if (cycleDay == 21) return "3 неделя цикла: воскресенье 50%";
            if (cycleDay == 22) return "4 неделя цикла: 15% -> 20%";
            if (cycleDay == 23) return "4 неделя цикла: 20% -> 30%";
            if (cycleDay == 24) return "4 неделя цикла: 30% -> 40%";
            if (cycleDay == 25) return "4 неделя цикла: 40% -> 50%";
            if (cycleDay == 26) return "4 неделя цикла: все по 500 RSD";
            return "4 неделя цикла: все по 350 RSD";
        }
        if (!isFinalDiscountWeek(today)) {
            return "прогрессивный этап по возрасту товара";
        }
        int dow = today.getDayOfWeek().getValue(); // Mon=1
        if (dow == 1) return "последняя неделя: 15% -> 20%";
        if (dow == 2) return "последняя неделя: 20% -> 30%";
        if (dow == 3) return "последняя неделя: 30% -> 40%";
        if (dow == 4) return "последняя неделя: 40% -> 50%";
        if (dow == 5) return "последняя неделя: все по 500";
        return "последняя неделя: все по 350";
    }

    private String buildDiscountGroupSummary(List<ProductCard> cards, LocalDate today) {
        int g0 = 0, g15 = 0, g20 = 0, g30 = 0, g40 = 0, g50 = 0, g500 = 0, g350 = 0, gOther = 0;
        for (ProductCard card : cards) {
            DiscountTarget target = calculateDiscountTarget(card, today);
            if (target == null) continue;
            if (target.fixedPriceRsd != null) {
                if (Math.abs(target.fixedPriceRsd - 500.0) < 0.01) g500++;
                else if (Math.abs(target.fixedPriceRsd - 350.0) < 0.01) g350++;
                else gOther++;
                continue;
            }
            int d = target.discountPercent;
            if (d <= 0) g0++;
            else if (d <= 15) g15++;
            else if (d <= 20) g20++;
            else if (d <= 30) g30++;
            else if (d <= 40) g40++;
            else if (d <= 50) g50++;
            else gOther++;
        }
        return "Группы по авто-скидкам сегодня:\n" +
                "0%: " + g0 + " | 15%: " + g15 + " | 20%: " + g20 + " | 30%: " + g30 + " | 40%: " + g40 + " | 50%: " + g50 +
                "\n500 RSD: " + g500 + " | 350 RSD: " + g350 + (gOther > 0 ? " | прочее: " + gOther : "");
    }

    private String describeDiscountDay(LocalDate today) {
        String dow;
        switch (today.getDayOfWeek()) {
            case MONDAY:
                dow = "Пн";
                break;
            case TUESDAY:
                dow = "Вт";
                break;
            case WEDNESDAY:
                dow = "Ср";
                break;
            case THURSDAY:
                dow = "Чт";
                break;
            case FRIDAY:
                dow = "Пт";
                break;
            case SATURDAY:
                dow = "Сб";
                break;
            default:
                dow = "Вс";
                break;
        }
        LocalDate reset = getDiscountResetStartDate();
        if (reset != null && !today.isBefore(reset)) {
            Integer cycleDay = getDiscountCycleDay(today);
            return dow + ", день месяца " + today.getDayOfMonth() + ", день цикла " + cycleDay + "/28";
        }
        return dow + ", день месяца " + today.getDayOfMonth();
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

    private void sendAddProductModeMenu(long chatId) {
        sendText(chatId,
                "Выберите способ добавления товара:",
                inlineSingleColumn(
                        button("📸 По фото (ИИ)", CB_ADD_PRODUCT_AI),
                        button("🧾 Без фото (POS)", CB_WITHOUT_PHOTO),
                        button("📝 Обычным способом", CB_ADD_PRODUCT_OLD),
                        button("⬅ Назад в меню", CB_MENU)
                ));
    }

    private void sendAiPhotoPrompt(long chatId, AdminSession session, boolean withoutPhotoMode) {
        int groups = session.aiPhotoGroups.size();
        int totalPhotos = 0;
        for (List<String> files : session.aiPhotoGroups.values()) {
            totalPhotos += files.size();
        }
        String text;
        if (withoutPhotoMode) {
            text = "🧾 Режим «Без фото»\n\n" +
                    "Отправьте альбомы с фото ценников (в каждом альбоме до 9 фото).\n" +
                    "Бот считает цену и артикул, затем покажет карточку для проверки.\n" +
                    "Групп товаров: " + groups + "\n" +
                    "Всего фото: " + totalPhotos + ".";
        } else {
            text = "🤖 Добавление товара по фото\n\n" +
                    "Отправляйте альбомы фото: каждый альбом (до 9 фото) = отдельный товар.\n" +
                    "Бот автоматически заполнит карточку (бренд, размер, цена" +
                    (isArticleEnabled() ? ", артикул" : "") +
                    ").\n" +
                    "Групп товаров: " + groups + "\n" +
                    "Всего фото: " + totalPhotos + ".";
        }
        if (groups <= 0) {
            text += "\n\nЗагрузите первое фото.";
            sendText(chatId, text, inlineSingleColumn(
                    button("Отменить", CB_CANCEL_FLOW)
            ));
            return;
        }
        text += "\n\nЕсли все группы добавлены, нажмите «Готово».";
        sendText(chatId, text, inlineSingleColumn(
                button("Готово", CB_DONE_PHOTOS),
                button("Отменить", CB_CANCEL_FLOW)
        ));
    }

    private int addPhotoToAiBatch(AdminSession session, Message message) {
        if (message == null || message.getPhoto() == null || message.getPhoto().isEmpty()) {
            return 0;
        }
        String groupKey = message.getMediaGroupId();
        if (groupKey == null || groupKey.isBlank()) {
            groupKey = "single:" + message.getMessageId();
        }
        List<String> group = session.aiPhotoGroups.computeIfAbsent(groupKey, k -> new ArrayList<>());
        if (group.size() >= 9) {
            return group.size();
        }
        PhotoSize best = selectBestPhoto(message.getPhoto());
        group.add(best.getFileId());
        return group.size();
    }

    private void buildDraftFromAi(long chatId, AdminSession session, boolean withoutPhotoMode) {
        if (session.aiProcessing) {
            sendText(chatId, "⏳ Уже обрабатываю фото, пожалуйста подождите.");
            return;
        }
        if (session.aiPhotoGroups.isEmpty()) {
            sendAiPhotoPrompt(chatId, session, withoutPhotoMode);
            return;
        }
        session.aiProcessing = true;
        List<List<String>> groups = new ArrayList<>();
        for (List<String> fileIds : session.aiPhotoGroups.values()) {
            if (fileIds != null && !fileIds.isEmpty()) {
                groups.add(new ArrayList<>(fileIds));
            }
        }
        sendText(chatId, "⏳ Анализирую товары через ИИ... Групп: " + groups.size());
        workers.submit(() -> {
            try {
                List<DraftData> drafts = new ArrayList<>();
                for (int gi = 0; gi < groups.size(); gi++) {
                    List<String> fileIds = groups.get(gi);
                    List<byte[]> images = new ArrayList<>();
                    List<String> aiFileIds = new ArrayList<>();
                    if (!fileIds.isEmpty()) {
                        aiFileIds.add(fileIds.get(0));
                        if (fileIds.size() > 1) {
                            String last = fileIds.get(fileIds.size() - 1);
                            if (!last.equals(fileIds.get(0))) {
                                aiFileIds.add(last);
                            }
                        }
                    }
                    for (String fileId : aiFileIds) {
                        try {
                            images.add(downloadFileBytes(fileId));
                        } catch (Exception e) {
                            log.warn("Failed to download image {} for AI draft batch", fileId, e);
                        }
                    }
                    if (images.isEmpty()) {
                        continue;
                    }

                    DraftData draft = requestDraftFromAi(images, withoutPhotoMode);
                    draft.mode = withoutPhotoMode ? DraftMode.POS_ONLY : DraftMode.ONLINE_WITH_PHOTO;
                    draft.photoFileIds = new ArrayList<>(fileIds);
                    draft.status = "ACTIVE";
                    if (draft.title == null || draft.title.isBlank()) {
                        draft.title = withoutPhotoMode ? "Без фото" : "Товар";
                    }
                    if (withoutPhotoMode) {
                        draft.title = "Без фото";
                        draft.size = "";
                    }
                    if (!isArticleEnabled()) {
                        draft.article = generateInternalArticle();
                    } else {
                        String digits = draft.article == null ? "" : draft.article.replaceAll("\\D", "");
                        draft.article = digits.matches("\\d{8}") ? digits : "";
                    }
                    if (draft.priceRsd <= 0) {
                        log.warn("AI draft has invalid price for batch item {}", gi + 1);
                        continue;
                    }
                    drafts.add(draft);
                }

                if (drafts.isEmpty()) {
                    sendText(chatId,
                            "❗ Не удалось подготовить карточки. Проверьте фото и попробуйте снова.",
                            inlineSingleColumn(
                                    button("⬅ Назад в меню", CB_MENU),
                                    button("Отменить", CB_CANCEL_FLOW)
                            ));
                    return;
                }

                session.aiPhotoGroups.clear();
                session.draftQueue.clear();
                session.draftQueue.addAll(drafts);
                session.draftQueueIndex = 0;
                session.draft = session.draftQueue.get(0);
                session.state = AdminState.DRAFT_EDIT_CHOICE;
                sendText(chatId, "✅ Карточки готовы: " + drafts.size() + ". Показываю товар 1/" + drafts.size() + ".");
                sendDraftPreview(chatId, session, true);
            } catch (Exception e) {
                log.warn("Failed to build AI draft", e);
                sendText(chatId,
                        "❌ Не удалось распознать данные с фото: " + e.getMessage(),
                        inlineSingleColumn(
                                button("⬅ Назад в меню", CB_MENU),
                                button("Отменить", CB_CANCEL_FLOW)
                        ));
            } finally {
                session.aiProcessing = false;
            }
        });
    }

    private DraftData requestDraftFromAi(List<byte[]> images, boolean withoutPhotoMode) throws IOException {
        boolean articleEnabled = isArticleEnabled();
        String prompt;
        if (withoutPhotoMode) {
            prompt = "Extract fields from price tag photos and return only JSON:\n" +
                    "{\n" +
                    "  \"brand\": \"Без фото\",\n" +
                    "  \"size\": \"\",\n" +
                    "  \"price_rsd\": \"number only\"" +
                    (articleEnabled ? ",\n  \"article\": \"8 digits or empty\"\n" : "\n") +
                    "}\n" +
                    "No markdown, no extra text.";
        } else {
            prompt = "Extract product card fields from photos and return only JSON:\n" +
                    "{\n" +
                    "  \"brand\": \"title/brand\",\n" +
                    "  \"item_type\": \"short type like top/leggings/hoodie/jeans\",\n" +
                    "  \"gender\": \"ženski or muški\",\n" +
                    "  \"size\": \"must include gender prefix, e.g. 'ženski XS' or 'muški L'\",\n" +
                    "  \"price_rsd\": \"number only\"" +
                    (articleEnabled ? ",\n  \"article\": \"8 digits or empty\"\n" : "\n") +
                    "}\n" +
                    "Priority rule for gender: determine gender by the color of the price tag.\n" +
                    "If the price tag is brown, use gender=muški.\n" +
                    "If the price tag is white, use gender=ženski.\n" +
                    "This price tag color rule has higher priority than the clothing shape or brand.\n" +
                    "Rules: if item is top/crop top/bralette/leggings/skirt/dress/bikini, use gender=ženski.\n" +
                    "No markdown, no extra text.";
        }

        IOException lastIo = null;
        List<AiProvider> providers = buildAiProviders();
        int attempts = providers.size();
        for (int i = 0; i < attempts; i++) {
            AiProvider provider = providers.get(i);
            String model = provider.model;
            String endpoint = provider.endpoint;
            try {
                String raw = kie.completeRaw(prompt, images, model, endpoint);
                JsonNode node = parseAiJson(raw);
                DraftData draft = new DraftData();
                draft.title = readAiString(node, "brand", withoutPhotoMode ? "Без фото" : "Товар");
                String itemType = readAiString(node, "item_type", "");
                String gender = readAiString(node, "gender", "");
                draft.size = normalizeSizeWithGender(readAiString(node, "size", ""), draft.title + " " + itemType + " " + gender);
                draft.priceRsd = parseAiPrice(node.path("price_rsd").asText(""));
                draft.article = readAiString(node, "article", "");
                if (withoutPhotoMode) {
                    draft.title = "Без фото";
                    draft.size = "";
                }
                return draft;
            } catch (IOException e) {
                lastIo = e;
                log.warn("AI draft parse failed model={} attempt={}/{}: {}", model, i + 1, attempts, e.getMessage());
                if (isTimeoutError(e)) {
                    // If primary/fallback timed out locally, do not fan out to next model:
                    // user explicitly requested to avoid extra model calls in this case.
                    throw e;
                }
            }
            if (i + 1 < attempts && config.kieRetryDelayMs > 0) {
                try {
                    Thread.sleep(config.kieRetryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (lastIo != null) {
            throw lastIo;
        }
        throw new IOException("No AI response");
    }

    private boolean isTimeoutError(Throwable error) {
        if (error == null) return false;
        if (error instanceof java.io.InterruptedIOException) return true;
        String msg = error.getMessage();
        if (msg == null) return false;
        String lower = msg.toLowerCase(Locale.ROOT);
        return lower.contains("timeout") || lower.contains("timed out");
    }

    private List<AiProvider> buildAiProviders() {
        List<AiProvider> providers = new ArrayList<>();
        addAiProvider(providers, config.kieModel, config.kieEndpointOverride);
        addAiProvider(providers, config.kieFallbackModel, config.kieFallbackEndpointOverride);
        addAiProvider(providers, config.kieSecondFallbackModel, config.kieSecondFallbackEndpointOverride);
        if (providers.isEmpty()) {
            providers.add(new AiProvider("gemini-3-flash", ""));
        }
        return providers;
    }

    private void addAiProvider(List<AiProvider> providers, String model, String endpoint) {
        if (model == null || model.isBlank()) {
            return;
        }
        String cleanModel = model.trim();
        for (AiProvider p : providers) {
            if (p.model.equalsIgnoreCase(cleanModel)) {
                return;
            }
        }
        providers.add(new AiProvider(cleanModel, endpoint == null ? "" : endpoint.trim()));
    }

    private JsonNode parseAiJson(String raw) throws IOException {
        String value = raw == null ? "" : raw.trim();
        int start = value.indexOf('{');
        int end = value.lastIndexOf('}');
        if (start >= 0 && end > start) {
            value = value.substring(start, end + 1);
        }
        return mapper.readTree(value);
    }

    private String readAiString(JsonNode node, String field, String fallback) {
        String v = node.path(field).asText("");
        if (v == null) v = "";
        v = v.trim();
        return v.isBlank() ? fallback : v;
    }

    private double parseAiPrice(String raw) {
        Double price = parsePriceInput(raw);
        if (price == null || price <= 0) {
            return 0;
        }
        return price;
    }

    private void sendDraftPreview(long chatId, AdminSession session, boolean includePhotos) {
        if (session.draft == null) {
            sendWelcomeMenu(chatId, "⚠️ Черновик не найден.");
            return;
        }
        DraftData draft = session.draft;
        String postCaption = buildDraftPostCaption(draft);
        if (includePhotos && draft.mode == DraftMode.ONLINE_WITH_PHOTO && draft.photoFileIds != null && !draft.photoFileIds.isEmpty()) {
            try {
                if (draft.photoFileIds.size() == 1) {
                    SendPhoto sendPhoto = new SendPhoto();
                    sendPhoto.setChatId(chatId);
                    sendPhoto.setPhoto(new InputFile(draft.photoFileIds.get(0)));
                    sendPhoto.setCaption(postCaption);
                    execute(sendPhoto);
                } else {
                    SendMediaGroup mediaGroup = new SendMediaGroup();
                    mediaGroup.setChatId(chatId);
                    List<InputMedia> media = new ArrayList<>();
                    for (int i = 0; i < draft.photoFileIds.size(); i++) {
                        InputMediaPhoto photo = new InputMediaPhoto();
                        photo.setMedia(draft.photoFileIds.get(i));
                        if (i == 0) {
                            photo.setCaption(postCaption);
                        }
                        media.add(photo);
                    }
                    mediaGroup.setMedias(media);
                    execute(mediaGroup);
                }
            } catch (Exception e) {
                log.warn("Failed to send draft photo preview", e);
            }
        }

        String preview = renderDraftPreviewText(draft);
        String messageText;
        if (includePhotos && draft.mode == DraftMode.ONLINE_WITH_PHOTO && draft.photoFileIds != null && !draft.photoFileIds.isEmpty()) {
            messageText = "🧾 Это финальная карточка для публикации.\nЕсли нужно, внесите правки.";
        } else {
            messageText = preview;
        }
        sendText(chatId,
                messageText,
                inlineSingleColumn(
                        button("✏️ Редактировать", CB_DRAFT_EDIT),
                        button("✅ Готово", CB_DRAFT_READY),
                        button("Отменить", CB_CANCEL_FLOW)
                ));
    }

    private String renderDraftPreviewText(DraftData draft) {
        String post = buildDraftPostCaption(draft);
        if (draft.mode == DraftMode.POS_ONLY) {
            return "🧾 Проверьте карточку:\n\n" + post +
                    "\n\nТовар будет опубликован только в Point of Sale (без Telegram и без витрины интернет-магазина).";
        }
        return "🧾 Проверьте карточку:\n\n" + post;
    }

    private String buildDraftPostCaption(DraftData draft) {
        StringBuilder sb = new StringBuilder();
        sb.append(draft.title == null || draft.title.isBlank() ? "Товар" : draft.title).append("\n");
        if (draft.size != null && !draft.size.isBlank()) {
            sb.append("Vel - ").append(draft.size).append("\n");
        }
        sb.append("Cena - ").append(formatRsd(draft.priceRsd)).append(" RSD");
        if (isArticleEnabled()) {
            String article = draft.article == null ? "" : draft.article.trim();
            if (article.matches("\\d{8}")) {
                sb.append("\nArtikal: ").append(article);
            } else {
                sb.append("\nArtikal: [не распознан]");
            }
        }
        if (draft.mode == DraftMode.ONLINE_WITH_PHOTO) {
            return buildTelegramPostCaption(sb.toString());
        }
        return sb.toString();
    }

    private void applyDraftFieldEdit(long chatId, AdminSession session, String text) {
        if (session.draft == null) {
            sendWelcomeMenu(chatId, "⚠️ Черновик не найден. Начните заново.");
            return;
        }
        DraftData draft = session.draft;
        String value = text == null ? "" : text.trim();
        if (session.state == AdminState.DRAFT_EDIT_BRAND_INPUT) {
            if (value.isBlank()) {
                sendText(chatId, "Введите непустой бренд/название.");
                return;
            }
            draft.title = value;
        } else if (session.state == AdminState.DRAFT_EDIT_SIZE_INPUT) {
            draft.size = normalizeSizeWithGender(value, draft.title);
        } else if (session.state == AdminState.DRAFT_EDIT_PRICE_INPUT) {
            Double price = parsePriceInput(value);
            if (price == null || price <= 0) {
                sendText(chatId, "Введите корректную цену, например: 1500");
                return;
            }
            draft.priceRsd = price;
        } else if (session.state == AdminState.DRAFT_EDIT_ARTICLE_INPUT) {
            if (!isArticleEnabled()) {
                sendText(chatId, "Артикулы сейчас отключены.");
                return;
            }
            String article = value.replaceAll("\\D", "");
            if (!article.matches("\\d{8}")) {
                sendText(chatId, "Артикул должен содержать ровно 8 цифр.");
                return;
            }
            ProductCard existing = db.findProductCardByArticle(article);
            if (existing != null && ("ACTIVE".equals(existing.status) || "RESERVED".equals(existing.status) || "POS_ONLY".equals(existing.status))) {
                sendText(chatId, "Этот артикул уже используется активным товаром.");
                return;
            }
            draft.article = article;
        }

        session.state = AdminState.DRAFT_EDIT_CHOICE;
        sendDraftPreview(chatId, session, false);
    }

    private void publishDraftNow(long chatId, AdminSession session) {
        if (session.draft == null) {
            sendWelcomeMenu(chatId, "⚠️ Черновик не найден. Начните заново.");
            return;
        }
        DraftData draft = session.draft.copy();
        if (draft.priceRsd <= 0) {
            sendText(chatId, "❗ Цена должна быть больше 0. Отредактируйте карточку.");
            return;
        }
        if (draft.mode == DraftMode.ONLINE_WITH_PHOTO && (draft.photoFileIds == null || draft.photoFileIds.isEmpty())) {
            sendText(chatId, "❗ Для публикации онлайн нужен хотя бы один снимок.");
            return;
        }
        if (isArticleEnabled()) {
            String article = draft.article == null ? "" : draft.article.replaceAll("\\D", "");
            if (!article.matches("\\d{8}")) {
                sendText(chatId, "❗ Укажите артикул из 8 цифр (через «Редактировать»).");
                return;
            }
            ProductCard existing = db.findProductCardByArticle(article);
            if (existing != null && ("ACTIVE".equals(existing.status) || "RESERVED".equals(existing.status) || "POS_ONLY".equals(existing.status))) {
                sendText(chatId, "❗ Этот артикул уже используется. Укажите другой.");
                return;
            }
            draft.article = article;
        } else {
            draft.article = generateInternalArticle();
        }

        sendText(chatId, "⏳ Публикую товар, подождите 3-10 секунд...");
        workers.submit(() -> {
            try {
                PublishedDraft result = publishDraftInternal(draft);
                StringBuilder sb = new StringBuilder("✅ Товар опубликован.\n");
                sb.append("Название: ").append(result.title).append("\n");
                sb.append("Цена: ").append(formatRsd(result.priceRsd)).append(" RSD");
                if (isArticleEnabled()) {
                    sb.append("\nАртикул: ").append(result.article);
                }
                if (draft.mode == DraftMode.POS_ONLY) {
                    sb.append("\nРежим: Point of Sale");
                }
                if (advanceToNextDraft(session)) {
                    sendText(chatId, sb + "\n\n➡️ Переходим к следующему товару.");
                    sendDraftPreview(chatId, session, true);
                } else {
                    resetSession(session);
                    sendWelcomeMenu(chatId, sb + "\n\n✅ Все товары обработаны.");
                }
            } catch (Exception e) {
                log.error("Failed to publish draft now", e);
                sendText(chatId, "❌ Ошибка публикации: " + e.getMessage());
            }
        });
    }

    private PublishedDraft publishDraftInternal(DraftData draft) throws Exception {
        List<byte[]> images = new ArrayList<>();
        if (draft.photoFileIds != null) {
            for (String fileId : draft.photoFileIds) {
                try {
                    images.add(downloadFileBytes(fileId));
                } catch (Exception e) {
                    log.warn("Failed to download draft photo {}", fileId, e);
                }
            }
        }
        if (draft.mode == DraftMode.ONLINE_WITH_PHOTO && images.isEmpty()) {
            throw new IOException("Не удалось загрузить фото товара");
        }

        String title = draft.title == null || draft.title.isBlank() ? (draft.mode == DraftMode.POS_ONLY ? "Без фото" : "Товар") : draft.title.trim();
        String size = draft.size == null ? "" : draft.size.trim();
        double price = draft.priceRsd;
        String article = draft.article == null ? "" : draft.article.trim();
        if (!isArticleEnabled()) {
            article = generateInternalArticle();
        }
        if (isArticleEnabled() && !article.matches("\\d{8}")) {
            throw new IOException("Артикул должен содержать 8 цифр");
        }
        if (isArticleEnabled()) {
            ProductCard existing = db.findProductCardByArticle(article);
            if (existing != null && ("ACTIVE".equals(existing.status) || "RESERVED".equals(existing.status) || "POS_ONLY".equals(existing.status))) {
                throw new IOException("Артикул уже используется");
            }
        }

        String status = draft.mode == DraftMode.POS_ONLY ? "POS_ONLY" : "ACTIVE";
        String captionCore = buildProductCaption(title, size, price, price, article, 0, null, status);
        String telegramCaption = buildTelegramPostCaption(captionCore);
        CategorySelection explicit = TextParser.detectExplicitCategories(draft.rawDescription);
        if (explicit.entries.isEmpty()) {
            explicit.add("Muško", null);
        }

        ShopifyClient.ProductPayload payload = new ShopifyClient.ProductPayload();
        payload.title = draft.mode == DraftMode.POS_ONLY ? "Без фото" : title;
        payload.bodyHtml = captionCore.replace("\n", "<br>");
        payload.priceEur = formatPriceForShopify(price);
        payload.size = size;
        payload.tags = buildTags(explicit, new Classification());
        payload.productType = "Без фото".equals(payload.title) ? "Без фото" : selectProductType(explicit);
        payload.images = images;
        if (isArticleEnabled()) {
            payload.barcode = article;
            payload.sku = article;
        }

        long productId = 0;
        PublishResult pub = null;
        try {
            productId = shopify.createProduct(payload);
            ensureShopifyBodyHasProductId(productId, payload.title, payload.bodyHtml, payload.priceEur, payload.size, payload.barcode);
            if (draft.mode == DraftMode.POS_ONLY) {
                shopify.publishProductToPosOnly(productId);
            } else if (config.shopifyPublishAll) {
                try {
                    shopify.publishProductToAll(productId);
                } catch (Exception e) {
                    log.warn("Failed to publish product {} to all channels", productId, e);
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

            if (draft.mode == DraftMode.ONLINE_WITH_PHOTO) {
                pub = publishToListingChat(telegramCaption, images);
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
            }

            db.upsertProductCard(new ProductCard(
                    productId,
                    pub == null ? "POS_ONLY" : pub.channelId,
                    pub == null ? 0 : pub.primaryMessageId,
                    pub == null ? null : pub.mediaGroupId,
                    title,
                    size,
                    (draft.rawDescription == null || draft.rawDescription.isBlank()) ? captionCore : draft.rawDescription,
                    article,
                    price,
                    price,
                    0,
                    null,
                    status,
                    Instant.now().getEpochSecond(),
                    Instant.now().getEpochSecond()
            ));

            return new PublishedDraft(productId, title, price, article, status);
        } catch (Exception e) {
            if (productId > 0) {
                try {
                    shopify.deleteProduct(productId);
                } catch (Exception ignored) {
                }
            }
            if (pub != null) {
                deleteTelegramByReference(pub.channelId, pub.primaryMessageId, pub.mediaGroupId);
            }
            throw e;
        }
    }

    private void addPublicationSlotFromInput(long chatId, AdminSession session, String rawInput) {
        Long publishAtEpoch = parseBelgradeDateInputToEpoch(rawInput);
        if (publishAtEpoch == null) {
            sendText(chatId, "Неверный формат. Используйте: 22.05, 20:00");
            return;
        }
        db.addPublicationSlot(publishAtEpoch, session.userId > 0 ? session.userId : null);
        session.state = AdminState.IDLE;
        sendPublicationSlotsPage(chatId, 0, "✅ Слот публикации добавлен.");
    }

    private void offerDraftScheduleBySlots(long chatId, AdminSession session) {
        long now = Instant.now().getEpochSecond();
        db.deletePublicationSlotsPast(now);
        List<Database.PublicationSlot> slots = db.listPublicationSlotsFuture(now, 100, 0);
        if (slots.isEmpty()) {
            sendText(chatId,
                    "⛔ Нет настроенных слотов публикации.\nСначала добавьте дату и время в меню планирования.",
                    inlineSingleColumn(
                            button("🗓 Планирование публикаций", CB_SCHEDULE_PLAN),
                            button("Отменить", CB_CANCEL_FLOW)
                    ));
            return;
        }
        if (slots.size() == 1) {
            scheduleDraftWithSlot(chatId, session, slots.get(0).id);
            return;
        }
        ZoneId belgrade = ZoneId.of("Europe/Belgrade");
        List<InlineKeyboardButton> buttons = new ArrayList<>();
        for (Database.PublicationSlot slot : slots) {
            String label = Instant.ofEpochSecond(slot.publishAt).atZone(belgrade).format(BELGRADE_DT_SHOW);
            buttons.add(button("🗓 " + label, "DRAFT:SLOT:" + slot.id));
        }
        buttons.add(button("Отменить", CB_CANCEL_FLOW));
        sendText(chatId,
                "Выберите слот публикации:",
                inlineSingleColumn(buttons.toArray(new InlineKeyboardButton[0])));
    }

    private void scheduleDraftWithSlot(long chatId, AdminSession session, long slotId) {
        Database.PublicationSlot slot = db.findPublicationSlotById(slotId);
        if (slot == null) {
            sendText(chatId, "Слот не найден.");
            offerDraftScheduleBySlots(chatId, session);
            return;
        }
        long now = Instant.now().getEpochSecond();
        if (slot.publishAt <= now) {
            db.deletePublicationSlot(slotId);
            sendText(chatId, "Этот слот уже в прошлом и был удален.");
            offerDraftScheduleBySlots(chatId, session);
            return;
        }
        ZoneId belgrade = ZoneId.of("Europe/Belgrade");
        String label = Instant.ofEpochSecond(slot.publishAt).atZone(belgrade).format(BELGRADE_DT_SHOW) + " (Belgrade)";
        enqueueCurrentDraftAt(chatId, session, slot.publishAt, label);
    }

    private void enqueueCurrentDraftAt(long chatId, AdminSession session, long publishAtEpoch, String showLabel) {
        if (session.draft == null) {
            sendWelcomeMenu(chatId, "⚠️ Черновик не найден. Начните заново.");
            return;
        }
        DraftData draft = session.draft.copy();
        if (!isArticleEnabled()) {
            draft.article = generateInternalArticle();
        } else {
            String article = draft.article == null ? "" : draft.article.replaceAll("\\D", "");
            if (!article.matches("\\d{8}")) {
                sendText(chatId, "❗ Для отложенной публикации укажите корректный артикул (8 цифр).");
                return;
            }
            draft.article = article;
        }
        try {
            ScheduledPayload payload = ScheduledPayload.fromDraft(draft);
            long taskId = db.enqueueScheduledPost(draft.mode.name(), mapper.writeValueAsString(payload), publishAtEpoch, chatId);
            String status = "✅ Пост поставлен в очередь.\nID: " + taskId +
                    "\nПубликация: " + showLabel;
            if (advanceToNextDraft(session)) {
                sendText(chatId, status + "\n\n➡️ Переходим к следующему товару.");
                sendDraftPreview(chatId, session, true);
            } else {
                resetSession(session);
                sendWelcomeMenu(chatId, status + "\n\n✅ Все товары обработаны.");
            }
        } catch (Exception e) {
            log.warn("Failed to enqueue scheduled draft", e);
            sendText(chatId, "❌ Не удалось поставить в очередь: " + e.getMessage());
        }
    }

    private Long parseBelgradeDateInputToEpoch(String rawInput) {
        String input = rawInput == null ? "" : rawInput.trim();
        try {
            String compact = input.replaceAll("\\s+", " ");
            java.util.regex.Matcher m = Pattern.compile("^(\\d{2})\\.(\\d{2}),\\s*(\\d{2}):(\\d{2})$").matcher(compact);
            if (!m.matches()) {
                return null;
            }
            int day = Integer.parseInt(m.group(1));
            int month = Integer.parseInt(m.group(2));
            int hour = Integer.parseInt(m.group(3));
            int minute = Integer.parseInt(m.group(4));
            ZoneId belgrade = ZoneId.of("Europe/Belgrade");
            int year = LocalDate.now(belgrade).getYear();
            LocalDateTime publishLocal = LocalDateTime.of(year, month, day, hour, minute);
            LocalDateTime nowBelgrade = LocalDateTime.now(belgrade);
            if (!publishLocal.isAfter(nowBelgrade)) {
                return null;
            }
            return publishLocal.atZone(belgrade).toEpochSecond();
        } catch (Exception e) {
            return null;
        }
    }

    private boolean advanceToNextDraft(AdminSession session) {
        if (session.draftQueue.isEmpty() || session.draftQueueIndex < 0) {
            return false;
        }
        int next = session.draftQueueIndex + 1;
        if (next >= session.draftQueue.size()) {
            session.draftQueue.clear();
            session.draftQueueIndex = -1;
            session.draft = null;
            return false;
        }
        session.draftQueueIndex = next;
        session.draft = session.draftQueue.get(next);
        session.state = AdminState.DRAFT_EDIT_CHOICE;
        return true;
    }

    private void sendPublicationSlotsPage(long chatId, int page, String statusMessage) {
        long now = Instant.now().getEpochSecond();
        db.deletePublicationSlotsPast(now);
        int pageSize = Math.max(1, config.listPageSize);
        int total = db.countPublicationSlotsFuture(now);
        int pages = Math.max(1, (int) Math.ceil(total / (double) pageSize));
        int safePage = Math.max(0, Math.min(page, pages - 1));
        List<Database.PublicationSlot> items = db.listPublicationSlotsFuture(now, pageSize, safePage * pageSize);

        ZoneId belgrade = ZoneId.of("Europe/Belgrade");
        StringBuilder sb = new StringBuilder();
        sb.append("🗓 Планирование публикаций\n");
        sb.append("Активных слотов: ").append(total).append("\n");
        sb.append("Страница ").append(safePage + 1).append("/").append(pages).append("\n");
        sb.append("Формат добавления: 22.05, 20:00 (Belgrade)\n");
        if (items.isEmpty()) {
            sb.append("\nСлоты не добавлены.");
        } else {
            for (Database.PublicationSlot slot : items) {
                String label = Instant.ofEpochSecond(slot.publishAt).atZone(belgrade).format(BELGRADE_DT_SHOW);
                sb.append("\n").append(slot.id).append(". ").append(label);
            }
        }
        if (statusMessage != null && !statusMessage.isBlank()) {
            sb.append("\n\n").append(statusMessage);
        }

        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        appendPaginationRows(rows, "SLOTS", safePage, pages);
        rows.add(List.of(button("➕ Добавить слот", CB_SLOT_ADD)));
        for (Database.PublicationSlot slot : items) {
            rows.add(List.of(button("🗑 Удалить слот #" + slot.id, "SLOT:DEL:" + slot.id)));
        }
        rows.add(List.of(button("⬅ Назад в меню", CB_MENU)));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        markup.setKeyboard(rows);
        sendText(chatId, sb.toString(), markup);
    }

    private void sendScheduledPostsPage(long chatId, int page) {
        int pageSize = Math.max(1, config.listPageSize);
        int total = db.countPendingScheduledPosts();
        int pages = Math.max(1, (int) Math.ceil(total / (double) pageSize));
        int safePage = Math.max(0, Math.min(page, pages - 1));
        List<Database.ScheduledPost> items = db.listPendingScheduledPosts(pageSize, safePage * pageSize);

        StringBuilder sb = new StringBuilder();
        sb.append("📅 Отложенные посты: ").append(total).append("\n");
        sb.append("Страница ").append(safePage + 1).append("/").append(pages).append("\n");
        if (items.isEmpty()) {
            sb.append("\nОчередь пуста.");
        } else {
            ZoneId belgrade = ZoneId.of("Europe/Belgrade");
            for (int i = 0; i < items.size(); i++) {
                Database.ScheduledPost sp = items.get(i);
                String modeLabel = "POS_ONLY".equalsIgnoreCase(sp.mode) ? "Без фото / POS" : "Онлайн";
                String title = "Товар";
                String price = "-";
                try {
                    ScheduledPayload payload = mapper.readValue(sp.payloadJson, ScheduledPayload.class);
                    if (payload.title != null && !payload.title.isBlank()) title = payload.title;
                    if (payload.priceRsd > 0) price = formatRsd(payload.priceRsd) + " RSD";
                } catch (Exception ignored) {
                }
                String dt = Instant.ofEpochSecond(sp.publishAt).atZone(belgrade).format(BELGRADE_DT_SHOW);
                sb.append("\n")
                        .append(sp.id)
                        .append(". ")
                        .append(dt)
                        .append(" | ")
                        .append(modeLabel)
                        .append(" | ")
                        .append(title)
                        .append(" | ")
                        .append(price);
            }
        }
        List<List<InlineKeyboardButton>> rows = new ArrayList<>();
        appendPaginationRows(rows, "SCHEDULED", safePage, pages);
        rows.add(List.of(button("⬅ Назад в меню", CB_MENU)));
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        markup.setKeyboard(rows);
        sendText(chatId, sb.toString(), markup);
    }

    private void processScheduledPostsSafe() {
        try {
            processScheduledPosts();
        } catch (Exception e) {
            log.warn("Scheduled posts sync failed", e);
        }
    }

    private void processScheduledPosts() {
        while (true) {
            List<Database.ScheduledPost> due = db.listDueScheduledPosts(Instant.now().getEpochSecond(), 20);
            if (due.isEmpty()) {
                return;
            }
            for (Database.ScheduledPost post : due) {
                try {
                    publishScheduledPostWithRetry(post);
                    db.markScheduledPostDone(post.id);
                } catch (Exception e) {
                    String err = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                    db.markScheduledPostRetry(post.id, err);
                    log.warn("Failed to publish scheduled post id={}: {}", post.id, err);
                    try {
                        sendText(post.createdBy, "⚠️ Не удалось опубликовать отложенный пост #" + post.id + ": " + err);
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private void publishScheduledPostWithRetry(Database.ScheduledPost post) throws Exception {
        int attempts = 6;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                ScheduledPayload payload = mapper.readValue(post.payloadJson, ScheduledPayload.class);
                DraftData draft = payload.toDraft();
                if (!isArticleEnabled()) {
                    draft.article = generateInternalArticle();
                }
                publishDraftInternal(draft);
                return;
            } catch (RateLimitException e) {
                long waitSec = Math.max(2, e.getRetryAfterSeconds() > 0 ? e.getRetryAfterSeconds() : 2);
                sleepQuietly(waitSec * 1000L);
            } catch (TelegramApiException e) {
                if (!contains429(e.getMessage()) || attempt == attempts) {
                    throw e;
                }
                sleepQuietly(2000L * attempt);
            } catch (IOException e) {
                if (!contains429(e.getMessage()) || attempt == attempts) {
                    throw e;
                }
                sleepQuietly(2000L * attempt);
            }
        }
        throw new IOException("Retries exhausted");
    }

    private boolean contains429(String message) {
        if (message == null) return false;
        String lower = message.toLowerCase(Locale.ROOT);
        return lower.contains("429") || lower.contains("too many requests") || lower.contains("rate limit");
    }

    private String withShopifyProductIdFooterHtml(String bodyHtml, long productId) {
        String base = bodyHtml == null ? "" : bodyHtml.trim();
        base = SHOPIFY_ID_FOOTER_PATTERN.matcher(base).replaceFirst("").trim();
        if (base.isBlank()) {
            return "ID: " + productId;
        }
        return base + "<br><br>ID: " + productId;
    }

    private void ensureShopifyBodyHasProductId(long productId, String title, String bodyHtml, String price, String size, String barcode) {
        if (productId <= 0 || price == null || price.isBlank()) {
            return;
        }
        try {
            ShopifyProductSnapshot snap = shopify.getProductSnapshot(productId);
            String currentBody = snap.bodyHtml == null ? "" : snap.bodyHtml;
            String desiredBody = withShopifyProductIdFooterHtml(
                    (bodyHtml == null || bodyHtml.isBlank()) ? currentBody : bodyHtml,
                    productId
            );
            if (desiredBody.equals(currentBody)) {
                return;
            }
            String finalTitle = (title == null || title.isBlank()) ? snap.title : title;
            String finalSize = size == null ? "" : size;
            String finalBarcode = (barcode == null || barcode.isBlank()) ? snap.variantBarcode : barcode;
            shopify.updateProduct(productId, snap.variantId, finalTitle, desiredBody, price, finalSize, finalBarcode);
        } catch (Exception e) {
            log.warn("Failed to set ID footer in Shopify description for product {}", productId, e);
        }
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handleArticulCommand(long chatId, String text) {
        String[] parts = text.trim().split("\\s+");
        if (parts.length == 1 || "status".equalsIgnoreCase(parts[1])) {
            sendText(chatId, "Артикулы: " + (isArticleEnabled() ? "включены ✅" : "отключены ⏸"));
            return;
        }
        String cmd = parts[1].toLowerCase(Locale.ROOT);
        if ("on".equals(cmd) || "enable".equals(cmd) || "1".equals(cmd)) {
            db.setMeta(META_ARTICLE_ENABLED, "true");
            sendText(chatId, "✅ Артикулы включены. Бот снова будет использовать артикулы и штрихкоды.");
            return;
        }
        if ("off".equals(cmd) || "disable".equals(cmd) || "0".equals(cmd)) {
            db.setMeta(META_ARTICLE_ENABLED, "false");
            sendText(chatId, "⏸ Артикулы отключены. Новые товары будут публиковаться без артикула в тексте.");
            return;
        }
        sendText(chatId, "Команда: /articul on | /articul off | /articul status");
    }

    private boolean isArticleEnabled() {
        String raw = db.getMeta(META_ARTICLE_ENABLED);
        return raw != null && "true".equalsIgnoreCase(raw.trim());
    }

    private String generateInternalArticle() {
        for (int i = 0; i < 200; i++) {
            String candidate = String.format(Locale.US, "%08d", ThreadLocalRandom.current().nextInt(10000000, 99999999));
            if (!db.existsActiveArticle(candidate)) {
                return candidate;
            }
        }
        String fallback = String.format(Locale.US, "%08d", (int) (Instant.now().getEpochSecond() % 100000000L));
        if (!db.existsActiveArticle(fallback)) {
            return fallback;
        }
        return String.format(Locale.US, "%08d", ThreadLocalRandom.current().nextInt(10000000, 99999999));
    }

    private static class PublishedDraft {
        final long productId;
        final String title;
        final double priceRsd;
        final String article;
        final String status;

        PublishedDraft(long productId, String title, double priceRsd, String article, String status) {
            this.productId = productId;
            this.title = title;
            this.priceRsd = priceRsd;
            this.article = article;
            this.status = status;
        }
    }

    private static class AiProvider {
        final String model;
        final String endpoint;

        AiProvider(String model, String endpoint) {
            this.model = model;
            this.endpoint = endpoint == null ? "" : endpoint;
        }
    }

    private static class ScheduledPayload {
        public String mode;
        public List<String> photoFileIds = new ArrayList<>();
        public String title;
        public String size;
        public double priceRsd;
        public String article;
        public String status;
        public String rawDescription;

        static ScheduledPayload fromDraft(DraftData draft) {
            ScheduledPayload payload = new ScheduledPayload();
            payload.mode = draft.mode == null ? DraftMode.ONLINE_WITH_PHOTO.name() : draft.mode.name();
            payload.photoFileIds = draft.photoFileIds == null ? new ArrayList<>() : new ArrayList<>(draft.photoFileIds);
            payload.title = draft.title;
            payload.size = draft.size;
            payload.priceRsd = draft.priceRsd;
            payload.article = draft.article;
            payload.status = draft.status;
            payload.rawDescription = draft.rawDescription;
            return payload;
        }

        DraftData toDraft() {
            DraftData draft = new DraftData();
            draft.mode = mode == null ? DraftMode.ONLINE_WITH_PHOTO : DraftMode.valueOf(mode);
            draft.photoFileIds = photoFileIds == null ? new ArrayList<>() : new ArrayList<>(photoFileIds);
            draft.title = title == null ? "" : title;
            draft.size = size == null ? "" : size;
            draft.priceRsd = priceRsd;
            draft.article = article == null ? "" : article;
            draft.status = status == null ? "ACTIVE" : status;
            draft.rawDescription = rawDescription == null ? "" : rawDescription;
            return draft;
        }
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
        return inlineRows(
                List.of(button("➕ Добавить товар", CB_ADD_PRODUCT)),
                List.of(button("🗓 Планирование публикаций", CB_SCHEDULE_PLAN)),
                List.of(
                        button("📅 Отложенные", CB_SCHEDULED_POSTS),
                        button("✏️ Редактировать", CB_EDIT_POST)
                ),
                List.of(button("📦 Список товаров", CB_PRODUCTS)),
                List.of(
                        button("🟡 Зарезервировать", CB_RESERVE),
                        button("🟢 Снять резерв", CB_UNRESERVE)
                ),
                List.of(
                        button("🧾 Скидки", CB_DISCOUNTS),
                        button("✅ Продано", CB_SOLD)
                )
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

    @SafeVarargs
    private final InlineKeyboardMarkup inlineRows(List<InlineKeyboardButton>... rows) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        markup.setKeyboard(List.of(rows));
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
        AdminSession session = sessions.computeIfAbsent(userId, k -> new AdminSession());
        session.userId = userId;
        return session;
    }

    private void resetSession(AdminSession session) {
        session.state = AdminState.IDLE;
        session.pendingPhotoFileIds.clear();
        session.aiPhotoGroups.clear();
        session.draftQueue.clear();
        session.draftQueueIndex = -1;
        session.selectedProductId = 0;
        session.searchScope = null;
        session.draft = null;
        session.aiProcessing = false;
    }

    private void syncDiscountsSafe() {
        if (!discountSyncRunning.compareAndSet(false, true)) {
            log.info("Discount sync skipped because another run is still in progress");
            return;
        }
        try {
            syncDiscounts();
        } catch (Exception e) {
            log.warn("Discount sync failed", e);
        } finally {
            discountSyncRunning.set(false);
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

    private String normalizeSizeWithGender(String rawSize, String contextText) {
        if (rawSize == null) return "";
        String value = rawSize.trim();
        if (value.isBlank()) return "";

        String lower = value.toLowerCase(Locale.ROOT);
        boolean female = containsFemaleHint(lower);
        boolean male = containsMaleHint(lower);

        String remainder = value
                .replaceAll("(?iu)\\b(vel|size)\\b\\s*[-:]?\\s*", "")
                .replaceAll("(?iu)\\b(muški|muski|muško|musko|ženski|zenski|žensko|zensko|female|male|women|woman|men|man|женск|мужск)\\b", "")
                .trim();
        remainder = remainder.replaceAll("^[-:]+\\s*", "").trim();

        if (!female && !male) {
            String ctx = contextText == null ? "" : contextText.toLowerCase(Locale.ROOT);
            female = containsFemaleHint(ctx);
            male = containsMaleHint(ctx);
        }
        String prefix = female ? "ženski" : "muški";
        if (remainder.isBlank()) {
            return prefix;
        }
        return prefix + " " + remainder;
    }

    private boolean containsFemaleHint(String text) {
        if (text == null || text.isBlank()) return false;
        return text.contains("žensk")
                || text.contains("zensk")
                || text.contains("female")
                || text.contains("women")
                || text.contains("woman")
                || text.contains("женск")
                || text.contains("top")
                || text.contains("crop")
                || text.contains("bralet")
                || text.contains("grudnjak")
                || text.contains("bikini")
                || text.contains("suknja")
                || text.contains("haljina")
                || text.contains("tajice")
                || text.contains("helanke")
                || text.contains("leggings");
    }

    private boolean containsMaleHint(String text) {
        if (text == null || text.isBlank()) return false;
        return text.contains("mušk")
                || text.contains("musk")
                || text.contains("male")
                || text.contains("men")
                || text.contains("man")
                || text.contains("мужск");
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

    private void applyDiscountCycleStartDate(long chatId, AdminSession session, String rawText) {
        String value = rawText == null ? "" : rawText.trim();
        LocalDate parsed = null;
        if (!value.isBlank()) {
            try {
                parsed = LocalDate.parse(value);
            } catch (DateTimeParseException ignored) {
            }
            if (parsed == null) {
                try {
                    parsed = LocalDate.parse(value, DateTimeFormatter.ofPattern("dd.MM.yyyy"));
                } catch (DateTimeParseException ignored) {
                }
            }
        }
        if (parsed == null) {
            sendText(chatId,
                    "Не удалось распознать дату.\nВведите в формате `2026-06-01` или `01.06.2026`.",
                    inlineSingleColumn(button("Отменить", CB_CANCEL_FLOW)));
            return;
        }
        db.setMeta(META_DISCOUNT_RESET_START, parsed.toString());
        db.setMeta("discount:last_sync_date", "");
        log.warn("Discount cycle start date changed manually by admin {}. New start date={}", session.userId, parsed);
        triggerDiscountSyncNow("date-set-manually");
        resetSession(session);
        sendDiscountsDashboard(chatId, "✅ Дата старта цикла установлена: " + parsed + "\nПересчет скидок запущен.");
    }

    private void triggerDiscountSyncNow(String reason) {
        try {
            workers.submit(() -> {
                log.info("Immediate discount sync requested: {}", reason);
                syncDiscountsSafe();
            });
        } catch (Exception e) {
            log.warn("Failed to trigger immediate discount sync: {}", reason, e);
        }
    }

    private void syncDiscounts() {
        if (!isDiscountsEnabled()) {
            return;
        }
        LocalDate today = todayInDiscountZone();
        String key = "discount:last_sync_date";
        String syncMarker = buildDiscountSyncMarker(today);
        String lastDate = db.getMeta(key);
        List<ProductCard> cards = db.listProductsForDiscount();
        if (syncMarker.equals(lastDate) && !hasDiscountDrift(cards, today)) {
            return;
        }
        if (cards.isEmpty()) {
            db.setMeta(key, syncMarker);
            return;
        }

        boolean retryNeeded = false;
        boolean transientFailure = false;
        for (ProductCard card : cards) {
            try {
                DiscountTarget target = calculateDiscountTarget(card, today);
                if (target == null) continue;
                if (!requiresDiscountSync(card, target)) {
                    continue;
                }
                boolean shouldBeInSale = target.discountPercent > 0 || target.fixedPriceRsd != null;
                boolean alreadyInSale = card.discountPercent > 0 || card.fixedPriceRsd != null;
                boolean syncSaleCollection = shouldBeInSale != alreadyInSale;
                syncCardState(card, card.status, target.discountPercent, target.fixedPriceRsd, target.currentPriceRsd, syncSaleCollection);
                sleepQuietly(Math.max(config.productSyncDelayMs, 1200));
            } catch (RateLimitException e) {
                retryNeeded = true;
                recordShopifyReadCooldown(e.getRetryAfterSeconds(), "discount-sync");
                log.warn("Discount sync rate limited by Shopify on product {}, will retry later", card.productId);
                break;
            } catch (Exception e) {
                log.warn("Failed to apply discount to product {}", card.productId, e);
                if (isTransientDiscountSyncFailure(e)) {
                    transientFailure = true;
                    break;
                }
                if (contains429(e.getMessage())) {
                    retryNeeded = true;
                    scheduleDiscountRetrySoon("429-during-discount-sync");
                    break;
                }
            }
        }
        if (retryNeeded || transientFailure) {
            scheduleDiscountRetrySoon("discount-sync-incomplete");
            return;
        }
        db.setMeta(key, syncMarker);
    }

    private boolean hasDiscountDrift(List<ProductCard> cards, LocalDate today) {
        for (ProductCard card : cards) {
            DiscountTarget target = calculateDiscountTarget(card, today);
            if (target != null && requiresDiscountSync(card, target)) {
                return true;
            }
        }
        return false;
    }

    private DiscountTarget calculateDiscountTarget(ProductCard card, LocalDate today) {
        if (card.basePriceRsd <= 0) return null;

        if (card.fixedPriceRsd != null && card.fixedPriceRsd <= 350.0) {
            return new DiscountTarget(discountPercentByFixed(card.basePriceRsd, 350.0), 350.0, 350.0);
        }

        LocalDate createdAtDate = Instant.ofEpochSecond(card.createdAt)
                .atZone(discountZone == null ? ZoneId.systemDefault() : discountZone)
                .toLocalDate();
        LocalDate ageStart = createdAtDate;
        int sundaySteps = countSundaySteps(ageStart, today);
        int baseDiscount = sundaySteps <= 0 ? 0 : sundaySteps == 1 ? 15 : sundaySteps == 2 ? 30 : 50;
        int discount = baseDiscount;
        Double fixed = null;

        if (isFinalDiscountWeek(today)) {
            int dow = today.getDayOfWeek().getValue(); // Mon=1
            if (dow == 5) {
                fixed = 500.0;
            } else if (dow >= 6) {
                fixed = 350.0;
            } else {
                discount = evolveFinalWeekDiscount(baseDiscount, dow);
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

    private boolean requiresDiscountSync(ProductCard card, DiscountTarget target) {
        boolean samePrice = Math.abs(card.currentPriceRsd - target.currentPriceRsd) < 0.00001;
        boolean sameDiscount = card.discountPercent == target.discountPercent;
        boolean sameFixed = (card.fixedPriceRsd == null && target.fixedPriceRsd == null) ||
                (card.fixedPriceRsd != null && target.fixedPriceRsd != null &&
                        Math.abs(card.fixedPriceRsd - target.fixedPriceRsd) < 0.00001);
        return !(samePrice && sameDiscount && sameFixed);
    }

    private int countSundaySteps(LocalDate start, LocalDate today) {
        if (start == null || today == null || today.isBefore(start)) return 0;
        LocalDate firstSunday = start.with(java.time.temporal.TemporalAdjusters.nextOrSame(java.time.DayOfWeek.SUNDAY));
        if (firstSunday.isAfter(today)) return 0;
        long days = java.time.temporal.ChronoUnit.DAYS.between(firstSunday, today);
        return (int) (days / 7L) + 1;
    }

    private int discountPercentByFixed(double basePrice, double fixedPrice) {
        if (basePrice <= 0) return 0;
        return Math.max(0, (int) Math.round((1.0 - (fixedPrice / basePrice)) * 100.0));
    }

    private int evolveFinalWeekDiscount(int baseDiscount, int dayOfWeek) {
        if (baseDiscount >= 50) {
            return 50;
        }
        if (baseDiscount >= 30) {
            if (dayOfWeek >= 3) {
                return 40;
            }
            return 30;
        }
        if (baseDiscount >= 15) {
            if (dayOfWeek == 1) {
                return 20;
            }
            if (dayOfWeek == 2) {
                return 30;
            }
            if (dayOfWeek == 3) {
                return 40;
            }
            if (dayOfWeek == 4) {
                return 50;
            }
            return 15;
        }
        return 0;
    }

    private String buildDiscountSyncMarker(LocalDate today) {
        Integer cycleDay = getDiscountCycleDay(today);
        return today + "|cycleDay=" + (cycleDay == null ? 0 : cycleDay) + "|final=" + isFinalDiscountWeek(today) + "|v2";
    }

    private void scheduleDiscountRetrySoon(String reason) {
        if (!discountSyncRetryScheduled.compareAndSet(false, true)) {
            return;
        }
        long delaySeconds = Math.max(60, config.shopifyRateLimitCooldownSeconds);
        scheduler.schedule(() -> {
            discountSyncRetryScheduled.set(false);
            log.info("Retrying discount sync after delay: {}", reason);
            syncDiscountsSafe();
        }, delaySeconds, TimeUnit.SECONDS);
    }

    private boolean isTransientDiscountSyncFailure(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof java.net.UnknownHostException ||
                    current instanceof java.net.SocketTimeoutException ||
                    current instanceof java.net.ConnectException ||
                    current instanceof javax.net.ssl.SSLException ||
                    current instanceof RateLimitException) {
                return true;
            }
            current = current.getCause();
        }
        String message = error.getMessage();
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("temporary failure in name resolution") ||
                normalized.contains("connection reset") ||
                normalized.contains("connect timed out") ||
                normalized.contains("read timed out") ||
                normalized.contains("failed to edit telegram caption");
    }

    private boolean isShopifyProductMissingError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.toLowerCase(Locale.ROOT).contains("shopify get failed: 404")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean isTelegramMessageMissingError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String normalized = message.toLowerCase(Locale.ROOT);
                if (normalized.contains("message to edit not found") ||
                        normalized.contains("message_id_invalid") ||
                        normalized.contains("message to delete not found")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean isAlreadySyncedSaleCollectionError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String normalized = message.toLowerCase(Locale.ROOT);
                if (normalized.contains("already exists in this collection")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
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
        AI_DRAFT_PHOTOS,
        WITHOUT_PHOTO_PHOTOS,
        ADD_PRODUCT_DESCRIPTION,
        ADD_ADMIN_ID,
        ARTICLE_SEARCH_INPUT,
        PRODUCT_ID_SEARCH_INPUT,
        DRAFT_EDIT_CHOICE,
        DRAFT_EDIT_BRAND_INPUT,
        DRAFT_EDIT_SIZE_INPUT,
        DRAFT_EDIT_PRICE_INPUT,
        DRAFT_EDIT_ARTICLE_INPUT,
        DRAFT_PUBLISH_CHOICE,
        SCHEDULE_SLOT_INPUT,
        DISCOUNT_CYCLE_DATE_INPUT,
        RESERVE_SELECT,
        UNRESERVE_SELECT,
        SOLD_SELECT,
        EDIT_SELECT,
        MANUAL_DISCOUNT_SELECT,
        MANUAL_DISCOUNT_INPUT,
        EDIT_DESCRIPTION_INPUT
    }

    private enum ProductSearchScope {
        PRODUCTS,
        RESERVE,
        UNRESERVE,
        SOLD,
        MANUAL,
        EDIT
    }

    private enum DraftMode {
        ONLINE_WITH_PHOTO,
        POS_ONLY
    }

    private static class AdminSession {
        AdminState state = AdminState.IDLE;
        final List<String> pendingPhotoFileIds = new ArrayList<>();
        final java.util.LinkedHashMap<String, List<String>> aiPhotoGroups = new java.util.LinkedHashMap<>();
        final List<DraftData> draftQueue = new ArrayList<>();
        int draftQueueIndex = -1;
        long selectedProductId = 0;
        ProductSearchScope searchScope;
        DraftData draft;
        long userId;
        volatile boolean aiProcessing;
    }

    private static class DraftData {
        DraftMode mode;
        List<String> photoFileIds = new ArrayList<>();
        String title = "";
        String size = "";
        double priceRsd = 0;
        String article = "";
        String status = "ACTIVE";
        String rawDescription = "";

        DraftData copy() {
            DraftData d = new DraftData();
            d.mode = this.mode;
            d.photoFileIds = this.photoFileIds == null ? new ArrayList<>() : new ArrayList<>(this.photoFileIds);
            d.title = this.title;
            d.size = this.size;
            d.priceRsd = this.priceRsd;
            d.article = this.article;
            d.status = this.status;
            d.rawDescription = this.rawDescription;
            return d;
        }
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

        String size = normalizeSizeWithGender(TextParser.extractSize(text), text);
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
        if (isArticleEnabled() && article != null && !article.isBlank()) {
            payload.barcode = article;
            payload.sku = article;
        }
        payload.tags = buildTags(resolved, ai);
        payload.productType = selectProductType(resolved);
        payload.images = images;

        long productId = shopify.createProduct(payload);
        ensureShopifyBodyHasProductId(productId, payload.title, payload.bodyHtml, payload.priceEur, payload.size, payload.barcode);
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
            String size = normalizeSizeWithGender(TextParser.extractSize(text), text);

            String priceEur = TextParser.extractEur(text);
            String priceRsd = TextParser.extractRsd(text);
            TextParser.DiscountInfo discount = TextParser.extractDiscount(text);
            PriceSelection priceSelection = selectPrice(priceEur, priceRsd, discount);
            String article = TextParser.extractArticle(text);

            String telegramLink = buildTelegramLink(channelId, messageId);
            String bodyHtml = buildDescription(text, "", priceSelection, discount, telegramLink);
            bodyHtml = withShopifyProductIdFooterHtml(bodyHtml, productId);
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
        List<AiProvider> providers = buildAiProviders();
        int attempts = Math.max(1, Math.min(config.kieRetryAttempts, providers.size()));
        long delayMs = Math.max(0, config.kieRetryDelayMs);
        Classification last = null;
        IOException lastIo = null;

        for (int attempt = 1; attempt <= attempts; attempt++) {
            AiProvider provider = providers.get(attempt - 1);
            String modelName = provider.model;
            try {
                Classification result = kie.classify(text, images, explicitHint, provider.model, provider.endpoint);
                last = result;
                int categoriesCount = (result == null || result.categories == null) ? 0 : result.categories.size();
                log.info("AI model {} success after {} attempt(s), categories={}", modelName, attempt, categoriesCount);
                // Do not continue to fallback when primary model already responded successfully.
                // Empty categories are handled later by default category resolution.
                return result == null ? new Classification() : result;
            } catch (IOException e) {
                lastIo = e;
                log.warn("AI model {} failed (attempt {}/{}): {}", modelName, attempt, attempts, e.getMessage());
                if (isTimeoutError(e)) {
                    throw e;
                }
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
        if (isShopifyReadCooldownActive()) {
            return;
        }
        boolean rateLimited = syncDeletedProductsBatch(
                META_SHOPIFY_SYNC_OFFSET,
                Math.max(1, config.productSyncBatchSize),
                db.countTelegramCardsForSync(),
                (limit, offset) -> db.listTelegramCardsForSync(limit, offset),
                "telegram"
        );
        if (!rateLimited && !isShopifyReadCooldownActive()) {
            syncDeletedProductsBatch(
                    META_SHOPIFY_POS_ONLY_SYNC_OFFSET,
                    Math.max(1, config.productSyncPosOnlyBatchSize),
                    db.countPosOnlyCardsForSync(),
                    (limit, offset) -> db.listPosOnlyCardsForSync(limit, offset),
                    "pos-only"
            );
        }
    }

    private boolean syncDeletedProductsBatch(String offsetMetaKey,
                                             int batchSize,
                                             int total,
                                             ProductSyncBatchLoader loader,
                                             String label) {
        if (total <= 0) {
            db.setMeta(offsetMetaKey, "0");
            return false;
        }
        int offset = readCursorOffset(offsetMetaKey, total);
        List<ProductCard> cards = loader.load(batchSize, offset);
        if (cards.isEmpty()) {
            db.setMeta(offsetMetaKey, "0");
            return false;
        }

        int nextOffset = offset;
        for (ProductCard card : cards) {
            try {
                ShopifyClient.ProductAvailability availability = shopify.getProductAvailability(card.productId);
                if (availability == ShopifyClient.ProductAvailability.MISSING) {
                    if (!markTelegramCardAsProdato(card)) {
                        log.warn("Product {} missing in Shopify, but Telegram caption update failed. Will retry later.", card.productId);
                        nextOffset++;
                        continue;
                    }
                    db.markProductStatus(card.productId, "SOLD");
                    db.deleteProductCard(card.productId);
                    log.info("Product missing in Shopify, marked as PRODATO and cleaned references. productId={}", card.productId);
                } else if (availability == ShopifyClient.ProductAvailability.OUT_OF_STOCK) {
                    try {
                        shopify.deleteProduct(card.productId);
                        log.info("Product {} deleted in Shopify after reaching 0 stock", card.productId);
                    } catch (Exception e) {
                        log.warn("Failed to delete out-of-stock product {} in Shopify", card.productId, e);
                    }
                    if (!markTelegramCardAsProdato(card)) {
                        log.warn("Product {} out of stock in Shopify, but Telegram caption update failed. Will retry later.", card.productId);
                        nextOffset++;
                        continue;
                    }
                    db.markProductStatus(card.productId, "SOLD");
                    db.deleteProductCard(card.productId);
                    log.info("Product out of stock in Shopify, marked as PRODATO and cleaned references. productId={}", card.productId);
                }
                if (config.productSyncDelayMs > 0) {
                    Thread.sleep(config.productSyncDelayMs);
                }
                nextOffset++;
            } catch (RateLimitException e) {
                recordShopifyReadCooldown(e.getRetryAfterSeconds(), "deleted-sync/" + label);
                db.setMeta(offsetMetaKey, String.valueOf(Math.min(nextOffset, Math.max(0, total - 1))));
                log.warn("Rate limited by Shopify during sync (productId={}), pausing until next cycle", card.productId);
                return true;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return true;
            } catch (Exception e) {
                log.warn("Failed to sync product {} existence", card.productId, e);
                nextOffset++;
            }
        }
        if (nextOffset >= total || cards.size() < batchSize) {
            nextOffset = 0;
        }
        db.setMeta(offsetMetaKey, String.valueOf(nextOffset));
        return false;
    }

    private int readCursorOffset(String metaKey, int total) {
        int offset = 0;
        String raw = db.getMeta(metaKey);
        if (raw != null && !raw.isBlank()) {
            try {
                offset = Integer.parseInt(raw.trim());
            } catch (NumberFormatException ignored) {
                offset = 0;
            }
        }
        if (offset < 0 || offset >= total) {
            return 0;
        }
        return offset;
    }

    private boolean isShopifyReadCooldownActive() {
        String raw = db.getMeta(META_SHOPIFY_READ_COOLDOWN_UNTIL);
        if (raw == null || raw.isBlank()) {
            return false;
        }
        try {
            long until = Long.parseLong(raw.trim());
            return until > Instant.now().getEpochSecond();
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private void recordShopifyReadCooldown(long retryAfterSeconds, String source) {
        long cooldown = retryAfterSeconds > 0 ? retryAfterSeconds : config.shopifyRateLimitCooldownSeconds;
        long until = Instant.now().getEpochSecond() + Math.max(5, cooldown);
        db.setMeta(META_SHOPIFY_READ_COOLDOWN_UNTIL, String.valueOf(until));
        log.warn("Shopify read cooldown set for {} seconds after {}", Math.max(5, cooldown), source);
    }

    @FunctionalInterface
    private interface ProductSyncBatchLoader {
        List<ProductCard> load(int limit, int offset);
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
                sendText(chatId, "✅ Резерв поставлен: " + card.title +
                        (isArticleEnabled() && card.article != null && !card.article.isBlank() ? " (Artikal: " + card.article + ")" : ""));
                sendSelectableProductsPage(chatId, session, 0);
                return;
            }
            if (session.state == AdminState.UNRESERVE_SELECT) {
                DiscountTarget target = calculateDiscountTarget(card, todayInDiscountZone());
                if (target == null) {
                    syncCardState(card, "ACTIVE", card.discountPercent, card.fixedPriceRsd, card.currentPriceRsd);
                } else {
                    syncCardState(card, "ACTIVE", target.discountPercent, target.fixedPriceRsd, target.currentPriceRsd);
                }
                ProductCard refreshed = db.findProductCardById(card.productId);
                String finalPrice = refreshed == null ? formatRsd(card.currentPriceRsd) : formatRsd(refreshed.currentPriceRsd);
                sendText(chatId, "✅ Резерв снят: " + card.title +
                        (isArticleEnabled() && card.article != null && !card.article.isBlank() ? " (Artikal: " + card.article + ")" : "") +
                        "\nАктуальная цена: " + finalPrice + " RSD");
                sendSelectableProductsPage(chatId, session, 0);
                return;
            }
            if (session.state == AdminState.SOLD_SELECT) {
                markCardAsSold(card);
                sendText(chatId, "✅ Товар помечен проданным: " + card.title +
                        (isArticleEnabled() && card.article != null && !card.article.isBlank() ? " (Artikal: " + card.article + ")" : ""));
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
                return;
            }
            if (session.state == AdminState.EDIT_SELECT) {
                beginEditFlow(chatId, session, card);
            }
        } catch (Exception e) {
            log.warn("Failed to process selection by ordinal for product {}", card.productId, e);
            sendText(chatId, "❌ Не удалось выполнить действие: " + e.getMessage());
        }
    }

    private void beginEditFlow(long chatId, AdminSession session, ProductCard card) {
        session.selectedProductId = card.productId;
        session.state = AdminState.EDIT_DESCRIPTION_INPUT;
        sendText(chatId,
                "✏️ Отправьте полностью новое описание товара.\n\n" +
                        "Можно изменить название, размер, цену и артикул.\n\n" +
                        "Пример:\nAdidas\nVel - muški L\nCena - 900 RSD\nArtikal: 56789356",
                inlineSingleColumn(
                        button("Отменить", CB_CANCEL_FLOW)
                ));
    }

    @Override
    public String getBotUsername() {
        return "ShopifyBridgeBot";
    }
}
