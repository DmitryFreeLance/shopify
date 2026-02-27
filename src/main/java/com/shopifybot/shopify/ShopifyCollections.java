package com.shopifybot.shopify;

import com.shopifybot.db.Database;

import java.io.IOException;
import java.util.*;

public class ShopifyCollections {
    private final ShopifyClient shopify;
    private final Database db;

    private final List<String> sections = Arrays.asList(
            "Muško",
            "Žensko",
            "Dečija kolekcija",
            "Sniženje"
    );

    private final List<String> subcategories = Arrays.asList(
            "Jakne",
            "Duksevi",
            "Džemperi",
            "Košulje",
            "Majice",
            "Farmerke",
            "Pantalone",
            "Donji deo trenerke",
            "Gornji deo trenerke",
            "Šorcevi"
    );

    public ShopifyCollections(ShopifyClient shopify, Database db) {
        this.shopify = shopify;
        this.db = db;
    }

    public void ensureCollections() throws IOException {
        Map<String, Long> existing = new HashMap<>();
        for (ShopifyClient.CustomCollection c : shopify.listAllCustomCollections()) {
            existing.put(c.title, c.id);
            db.storeCollection(c.title, c.id);
        }

        List<String> required = new ArrayList<>();
        required.addAll(sections);
        for (String section : Arrays.asList("Muško", "Žensko")) {
            for (String sub : subcategories) {
                if (section.equals("Žensko") && ("Donji deo trenerke".equals(sub) || "Gornji deo trenerke".equals(sub) || "Šorcevi".equals(sub))) {
                    continue;
                }
                required.add(section + " / " + sub);
            }
        }

        for (String title : required) {
            if (!existing.containsKey(title)) {
                ShopifyClient.CustomCollection created = shopify.createCustomCollection(title);
                db.storeCollection(created.title, created.id);
            }
        }
    }

    public Long getCollectionId(String title) {
        Long id = db.findCollectionId(title);
        if (id != null) return id;
        return null;
    }

    public List<String> buildCollectionTitles(String section, String subcategory) {
        List<String> titles = new ArrayList<>();
        if (section != null && !section.isBlank()) {
            titles.add(section);
            if (subcategory != null && !subcategory.isBlank() && (section.equals("Muško") || section.equals("Žensko"))) {
                titles.add(section + " / " + subcategory);
            }
        }
        return titles;
    }
}
