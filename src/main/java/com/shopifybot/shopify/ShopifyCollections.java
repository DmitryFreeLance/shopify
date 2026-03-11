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
            "Sniženje"
    );

    public ShopifyCollections(ShopifyClient shopify, Database db) {
        this.shopify = shopify;
        this.db = db;
    }

    public void ensureCollections() throws IOException {
        Map<String, Long> existing = new HashMap<>();
        for (ShopifyClient.CustomCollection c : shopify.listAllCustomCollections()) {
            existing.put(c.title, c.id);
            db.storeCollection(c.title, c.id, c.handle);
        }
    }

    public void ensureCatalogMenu() throws IOException {
        // Intentionally no-op. Menu structure is managed manually.
    }

    public Long getCollectionId(String title) {
        Long id = db.findCollectionId(title);
        if (id != null) return id;
        return null;
    }

    public String getCollectionHandle(String title) {
        return db.findCollectionHandle(title);
    }

    public List<String> buildCollectionTitles(String section, String subcategory) {
        List<String> titles = new ArrayList<>();
        if (section != null && !section.isBlank()) {
            titles.add(section);
        }
        return titles;
    }

    public List<Long> listAllCollectionIdsExcept(String excludeTitle) {
        List<Long> ids = new ArrayList<>();
        for (String title : sections) {
            if (title.equals(excludeTitle) || title.startsWith(excludeTitle + " / ")) continue;
            Long id = db.findCollectionId(title);
            if (id != null) ids.add(id);
        }
        return ids;
    }

    private List<MenuItemInput> buildCatalogMenuItems() {
        List<MenuItemInput> items = new ArrayList<>();
        for (String section : sections) {
            Long sectionId = getCollectionId(section);
            if (sectionId == null) continue;
            MenuItemInput sectionItem = MenuItemInput.collection(section, sectionId);
            items.add(sectionItem);
        }
        return items;
    }

    public List<String> listSectionTitles() {
        return Collections.unmodifiableList(sections);
    }

}
