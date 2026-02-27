package com.shopifybot;

import com.shopifybot.db.Database;
import com.shopifybot.telegram.ShopifyBot;
import okhttp3.OkHttpClient;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;

public class Main {
    public static void main(String[] args) throws Exception {
        Config config = new Config();

        Database db = new Database(config.sqlitePath);
        db.init();

        OkHttpClient http = new OkHttpClient.Builder()
                .callTimeout(config.kieTimeout)
                .build();

        ShopifyBot bot = new ShopifyBot(config, db, http);
        bot.initialize();

        TelegramBotsApi botsApi = new TelegramBotsApi(DefaultBotSession.class);
        botsApi.registerBot(bot);

        System.out.println("Bot started");
    }
}
