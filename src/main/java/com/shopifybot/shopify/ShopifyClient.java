package com.shopifybot.shopify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class ShopifyClient {
    private static final Logger log = LoggerFactory.getLogger(ShopifyClient.class);
    private static final MediaType JSON = MediaType.parse("application/json");
    private final OkHttpClient http;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String shopDomain;
    private final String apiVersion;
    private final TokenProvider tokenProvider;

    public ShopifyClient(OkHttpClient http, String shopDomain, String apiVersion, TokenProvider tokenProvider) {
        this.http = http;
        this.shopDomain = shopDomain;
        this.apiVersion = apiVersion;
        this.tokenProvider = tokenProvider;
    }

    public List<CustomCollection> listAllCustomCollections() throws IOException {
        List<CustomCollection> all = new ArrayList<>();
        long sinceId = 0;
        while (true) {
            String url = restBase() + "/custom_collections.json?limit=250" + (sinceId > 0 ? "&since_id=" + sinceId : "");
            JsonNode root = get(url);
            ArrayNode arr = (ArrayNode) root.path("custom_collections");
            if (arr == null || arr.isEmpty()) break;
            long maxId = sinceId;
            for (JsonNode node : arr) {
                long id = node.path("id").asLong();
                String title = node.path("title").asText();
                String handle = node.path("handle").asText();
                all.add(new CustomCollection(id, title, handle));
                if (id > maxId) maxId = id;
            }
            if (arr.size() < 250) break;
            sinceId = maxId;
        }
        return all;
    }

    public CustomCollection createCustomCollection(String title) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode collection = root.putObject("custom_collection");
        collection.put("title", title);
        collection.put("published", true);

        JsonNode response = post(restBase() + "/custom_collections.json", root);
        JsonNode created = response.path("custom_collection");
        return new CustomCollection(
                created.path("id").asLong(),
                created.path("title").asText(),
                created.path("handle").asText()
        );
    }

    public long createProduct(ProductPayload payload) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode product = root.putObject("product");
        product.put("title", payload.title);
        product.put("body_html", payload.bodyHtml);
        product.put("status", "active");
        if (payload.vendor != null && !payload.vendor.isBlank()) {
            product.put("vendor", payload.vendor);
        }
        if (payload.productType != null && !payload.productType.isBlank()) {
            product.put("product_type", payload.productType);
        }
        if (payload.tags != null && !payload.tags.isEmpty()) {
            product.put("tags", String.join(",", payload.tags));
        }

        if (payload.size != null && !payload.size.isBlank()) {
            ArrayNode options = product.putArray("options");
            ObjectNode option = options.addObject();
            option.put("name", "Size");

            ArrayNode variants = product.putArray("variants");
            ObjectNode variant = variants.addObject();
            variant.put("option1", payload.size);
            variant.put("price", payload.priceEur);
        } else {
            ArrayNode variants = product.putArray("variants");
            ObjectNode variant = variants.addObject();
            variant.put("price", payload.priceEur);
        }

        ArrayNode images = product.putArray("images");
        for (byte[] image : payload.images) {
            ObjectNode img = images.addObject();
            img.put("attachment", Base64.getEncoder().encodeToString(image));
        }

        JsonNode response = post(restBase() + "/products.json", root);
        JsonNode created = response.path("product");
        return created.path("id").asLong();
    }

    public void addProductToCollection(long productId, long collectionId) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode collect = root.putObject("collect");
        collect.put("product_id", productId);
        collect.put("collection_id", collectionId);
        post(restBase() + "/collects.json", root);
    }

    public void deleteProduct(long productId) throws IOException {
        Request request = new Request.Builder()
                .url(restBase() + "/products/" + productId + ".json")
                .addHeader("X-Shopify-Access-Token", tokenProvider.getToken())
                .delete()
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Shopify delete failed: " + response.code() + " " + response.message());
            }
        }
    }

    public void deleteCustomCollection(long collectionId) throws IOException {
        Request request = new Request.Builder()
                .url(restBase() + "/custom_collections/" + collectionId + ".json")
                .addHeader("X-Shopify-Access-Token", tokenProvider.getToken())
                .delete()
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Shopify delete custom collection failed: " + response.code() + " " + response.message());
            }
        }
    }

    public String getShopCurrency() throws IOException {
        JsonNode root = get(restBase() + "/shop.json");
        return root.path("shop").path("currency").asText("");
    }

    public ShopifyProductSnapshot getProductSnapshot(long productId) throws IOException {
        JsonNode root = get(restBase() + "/products/" + productId + ".json");
        JsonNode product = root.path("product");
        JsonNode variants = product.path("variants");
        long variantId = 0;
        if (variants.isArray() && variants.size() > 0) {
            variantId = variants.get(0).path("id").asLong(0);
        }
        return new ShopifyProductSnapshot(
                product.path("id").asLong(0),
                variantId,
                product.path("title").asText(""),
                product.path("tags").asText(""),
                product.path("product_type").asText("")
        );
    }

    public void updateProduct(long productId, long variantId, String title, String bodyHtml, String price, String size) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode product = root.putObject("product");
        product.put("id", productId);
        if (title != null && !title.isBlank()) {
            product.put("title", title);
        }
        if (bodyHtml != null && !bodyHtml.isBlank()) {
            product.put("body_html", bodyHtml);
        }
        ArrayNode variants = product.putArray("variants");
        ObjectNode variant = variants.addObject();
        if (variantId > 0) {
            variant.put("id", variantId);
        }
        if (size != null && !size.isBlank()) {
            variant.put("option1", size);
        }
        variant.put("price", price);

        Request request = new Request.Builder()
                .url(restBase() + "/products/" + productId + ".json")
                .addHeader("X-Shopify-Access-Token", tokenProvider.getToken())
                .put(RequestBody.create(mapper.writeValueAsBytes(root), JSON))
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Shopify update failed: " + response.code() + " " + response.message());
            }
        }
    }

    public boolean productExists(long productId) throws IOException {
        Request request = new Request.Builder()
                .url(restBase() + "/products/" + productId + ".json")
                .addHeader("X-Shopify-Access-Token", tokenProvider.getToken())
                .get()
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (response.code() == 404) return false;
            if (!response.isSuccessful()) {
                throw new IOException("Shopify GET product failed: " + response.code() + " " + response.message());
            }
            return true;
        }
    }

    public List<String> listPublicationIds() throws IOException {
        String query = "{ publications(first: 50) { edges { node { id name } } } }";
        JsonNode root = graphQL(query);
        List<String> ids = new ArrayList<>();
        for (JsonNode edge : root.path("data").path("publications").path("edges")) {
            ids.add(edge.path("node").path("id").asText());
        }
        return ids;
    }

    public void publishProductToAll(long productId) throws IOException {
        List<String> publications = listPublicationIds();
        if (publications.isEmpty()) return;
        String productGid = "gid://shopify/Product/" + productId;
        for (String pubId : publications) {
            String mutation = "mutation Publish($id: ID!, $pub: ID!) { publishablePublish(id: $id, input: [{publicationId: $pub}]) { userErrors { field message } } }";
            ObjectNode variables = mapper.createObjectNode();
            variables.put("id", productGid);
            variables.put("pub", pubId);
            graphQL(mutation, variables);
        }
    }

    public void removeProductFromCollection(long collectionId, long productId) throws IOException {
        String mutation = "mutation Remove($id: ID!, $productIds: [ID!]!) { collectionRemoveProducts(id: $id, productIds: $productIds) { userErrors { field message } } }";
        ObjectNode variables = mapper.createObjectNode();
        variables.put("id", "gid://shopify/Collection/" + collectionId);
        ArrayNode productIds = variables.putArray("productIds");
        productIds.add("gid://shopify/Product/" + productId);
        graphQL(mutation, variables);
    }

    public void setProductMetafield(long productId, String namespace, String key, String value, String type) throws IOException {
        if (value == null || value.isBlank()) return;
        String normalizedType = type == null ? "" : type.trim();
        String payloadValue = value;
        if ("link".equalsIgnoreCase(normalizedType)) {
            String trimmed = value.trim();
            if (!trimmed.startsWith("{")) {
                ObjectNode link = mapper.createObjectNode();
                link.put("url", value);
                link.put("text", "Telegram");
                payloadValue = mapper.writeValueAsString(link);
            }
        }
        String mutation = "mutation Set($metafields: [MetafieldsSetInput!]!) { metafieldsSet(metafields: $metafields) { userErrors { field message } } }";
        ObjectNode variables = mapper.createObjectNode();
        ArrayNode metafields = variables.putArray("metafields");
        ObjectNode mf = metafields.addObject();
        mf.put("ownerId", "gid://shopify/Product/" + productId);
        mf.put("namespace", namespace);
        mf.put("key", key);
        mf.put("type", type);
        mf.put("value", payloadValue);
        JsonNode root = graphQL(mutation, variables);
        JsonNode errors = root.path("data").path("metafieldsSet").path("userErrors");
        if (errors.isArray() && errors.size() > 0) {
            log.warn("Failed to set metafield {}.{} for product {}: {}", namespace, key, productId, errors.toString());
        }
    }

    public String findMenuIdByHandle(String handle) throws IOException {
        String query = "{ menus(first: 50) { nodes { id handle title } } }";
        JsonNode root = graphQL(query);
        for (JsonNode node : root.path("data").path("menus").path("nodes")) {
            if (handle.equalsIgnoreCase(node.path("handle").asText())) {
                return node.path("id").asText();
            }
        }
        return null;
    }

    public List<MenuSummary> listMenus() throws IOException {
        String query = "{ menus(first: 50) { nodes { handle title } } }";
        JsonNode root = graphQL(query);
        List<MenuSummary> menus = new ArrayList<>();
        for (JsonNode node : root.path("data").path("menus").path("nodes")) {
            menus.add(new MenuSummary(node.path("handle").asText(), node.path("title").asText()));
        }
        return menus;
    }

    public void createOrUpdateMenu(String handle, String title, List<MenuItemInput> items) throws IOException {
        String menuId = findMenuIdByHandle(handle);
        ObjectNode variables = mapper.createObjectNode();
        variables.put("handle", handle);
        variables.put("title", title);
        variables.set("items", buildMenuItems(items));

        if (menuId == null || menuId.isBlank()) {
            String mutation = "mutation Create($handle: String!, $title: String!, $items: [MenuItemCreateInput!]!) { menuCreate(handle: $handle, title: $title, items: $items) { menu { id handle title } userErrors { field message } } }";
            graphQL(mutation, variables);
        } else {
            variables.put("id", menuId);
            String mutation = "mutation Update($id: ID!, $handle: String!, $title: String!, $items: [MenuItemUpdateInput!]!) { menuUpdate(id: $id, handle: $handle, title: $title, items: $items) { menu { id handle title } userErrors { field message } } }";
            graphQL(mutation, variables);
        }
    }

    public Menu getMenuByHandle(String handle) throws IOException {
        String query = "query Menu($handle: String!) { menu(handle: $handle) { id handle title items { title type url resourceId items { title type url resourceId items { title type url resourceId } } } } }";
        ObjectNode vars = mapper.createObjectNode();
        vars.put("handle", handle);
        JsonNode root = graphQL(query, vars);
        JsonNode menuNode = root.path("data").path("menu");
        if (menuNode.isMissingNode() || menuNode.isNull()) return null;
        return parseMenu(menuNode);
    }

    private JsonNode get(String url) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .addHeader("X-Shopify-Access-Token", tokenProvider.getToken())
                .get()
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Shopify GET failed: " + response.code() + " " + response.message());
            }
            return mapper.readTree(response.body().string());
        }
    }

    private JsonNode post(String url, ObjectNode body) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .addHeader("X-Shopify-Access-Token", tokenProvider.getToken())
                .post(RequestBody.create(mapper.writeValueAsBytes(body), JSON))
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Shopify POST failed: " + response.code() + " " + response.message());
            }
            return mapper.readTree(response.body().string());
        }
    }

    private JsonNode graphQL(String query) throws IOException {
        return graphQL(query, null);
    }

    private JsonNode graphQL(String query, ObjectNode variables) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        root.put("query", query);
        if (variables != null) {
            root.set("variables", variables);
        }
        Request request = new Request.Builder()
                .url(graphqlBase())
                .addHeader("X-Shopify-Access-Token", tokenProvider.getToken())
                .post(RequestBody.create(mapper.writeValueAsBytes(root), JSON))
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Shopify GraphQL failed: " + response.code() + " " + response.message());
            }
            return mapper.readTree(response.body().string());
        }
    }

    private ArrayNode buildMenuItems(List<MenuItemInput> items) {
        ArrayNode array = mapper.createArrayNode();
        for (MenuItemInput item : items) {
            ObjectNode node = mapper.createObjectNode();
            node.put("title", item.title);
            node.put("type", item.type);
            if ("HTTP".equalsIgnoreCase(item.type)) {
                if (item.url != null && !item.url.isBlank()) {
                    node.put("url", item.url);
                }
            } else if (item.resourceId != null && !item.resourceId.isBlank()) {
                node.put("resourceId", item.resourceId);
            }
            if (!item.items.isEmpty()) {
                node.set("items", buildMenuItems(item.items));
            }
            array.add(node);
        }
        return array;
    }

    private String restBase() {
        return "https://" + shopDomain + "/admin/api/" + apiVersion;
    }

    private String graphqlBase() {
        return "https://" + shopDomain + "/admin/api/" + apiVersion + "/graphql.json";
    }

    private Menu parseMenu(JsonNode node) {
        String id = node.path("id").asText();
        String handle = node.path("handle").asText();
        String title = node.path("title").asText();
        List<MenuItem> items = parseMenuItems(node.path("items"));
        return new Menu(id, handle, title, items);
    }

    private List<MenuItem> parseMenuItems(JsonNode itemsNode) {
        List<MenuItem> items = new ArrayList<>();
        if (itemsNode == null || !itemsNode.isArray()) return items;
        for (JsonNode itemNode : itemsNode) {
            String title = itemNode.path("title").asText();
            String type = itemNode.path("type").asText();
            String url = itemNode.path("url").asText();
            String resourceId = itemNode.path("resourceId").asText();
            List<MenuItem> children = parseMenuItems(itemNode.path("items"));
            items.add(new MenuItem(title, type, url, resourceId, children));
        }
        return items;
    }

    public static class CustomCollection {
        public final long id;
        public final String title;
        public final String handle;

        public CustomCollection(long id, String title, String handle) {
            this.id = id;
            this.title = title;
            this.handle = handle;
        }
    }

    public static class ProductPayload {
        public String title;
        public String bodyHtml;
        public String vendor;
        public String productType;
        public String priceEur;
        public String size;
        public List<String> tags;
        public List<byte[]> images;
    }

    public static class Menu {
        public final String id;
        public final String handle;
        public final String title;
        public final List<MenuItem> items;

        public Menu(String id, String handle, String title, List<MenuItem> items) {
            this.id = id;
            this.handle = handle;
            this.title = title;
            this.items = items;
        }
    }

    public static class MenuItem {
        public final String title;
        public final String type;
        public final String url;
        public final String resourceId;
        public final List<MenuItem> items;

        public MenuItem(String title, String type, String url, String resourceId, List<MenuItem> items) {
            this.title = title;
            this.type = type;
            this.url = url;
            this.resourceId = resourceId;
            this.items = items;
        }
    }

    public static class MenuSummary {
        public final String handle;
        public final String title;

        public MenuSummary(String handle, String title) {
            this.handle = handle;
            this.title = title;
        }
    }
}
