package com.shopifybot.shopify;

import java.util.ArrayList;
import java.util.List;

public class MenuItemInput {
    public final String title;
    public final String type;
    public final String resourceId;
    public final String url;
    public final List<MenuItemInput> items = new ArrayList<>();

    public MenuItemInput addChild(MenuItemInput child) {
        this.items.add(child);
        return this;
    }

    public static MenuItemInput http(String title, String url) {
        return new MenuItemInput(title, "HTTP", null, url);
    }

    public static MenuItemInput collection(String title, long collectionId) {
        return new MenuItemInput(title, "COLLECTION", "gid://shopify/Collection/" + collectionId, null);
    }

    public static MenuItemInput resource(String title, String type, String resourceId) {
        return new MenuItemInput(title, type, resourceId, null);
    }

    private MenuItemInput(String title, String type, String resourceId, String url) {
        this.title = title;
        this.type = type;
        this.resourceId = resourceId;
        this.url = url;
    }
}
