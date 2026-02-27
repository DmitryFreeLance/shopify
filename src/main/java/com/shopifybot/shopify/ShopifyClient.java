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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class ShopifyClient {
    private static final MediaType JSON = MediaType.parse("application/json");
    private final OkHttpClient http;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String shopDomain;
    private final String apiVersion;
    private final String adminToken;

    public ShopifyClient(OkHttpClient http, String shopDomain, String apiVersion, String adminToken) {
        this.http = http;
        this.shopDomain = shopDomain;
        this.apiVersion = apiVersion;
        this.adminToken = adminToken;
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
                all.add(new CustomCollection(id, title));
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
        return new CustomCollection(created.path("id").asLong(), created.path("title").asText());
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
                .addHeader("X-Shopify-Access-Token", adminToken)
                .delete()
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Shopify delete failed: " + response.code() + " " + response.message());
            }
        }
    }

    public boolean productExists(long productId) throws IOException {
        Request request = new Request.Builder()
                .url(restBase() + "/products/" + productId + ".json")
                .addHeader("X-Shopify-Access-Token", adminToken)
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

    private JsonNode get(String url) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .addHeader("X-Shopify-Access-Token", adminToken)
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
                .addHeader("X-Shopify-Access-Token", adminToken)
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
                .addHeader("X-Shopify-Access-Token", adminToken)
                .post(RequestBody.create(mapper.writeValueAsBytes(root), JSON))
                .build();
        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Shopify GraphQL failed: " + response.code() + " " + response.message());
            }
            return mapper.readTree(response.body().string());
        }
    }

    private String restBase() {
        return "https://" + shopDomain + "/admin/api/" + apiVersion;
    }

    private String graphqlBase() {
        return "https://" + shopDomain + "/admin/api/" + apiVersion + "/graphql.json";
    }

    public static class CustomCollection {
        public final long id;
        public final String title;

        public CustomCollection(long id, String title) {
            this.id = id;
            this.title = title;
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
}
