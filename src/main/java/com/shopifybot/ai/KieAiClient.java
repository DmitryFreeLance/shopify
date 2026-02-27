package com.shopifybot.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class KieAiClient {
    private static final MediaType JSON = MediaType.parse("application/json");
    private final OkHttpClient http;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String apiKey;
    private final String baseUrl;
    private final String endpointOverride;
    private final String model;

    public KieAiClient(OkHttpClient http, String apiKey, String model, String baseUrl, String endpointOverride) {
        this.http = http;
        this.apiKey = apiKey;
        this.model = model;
        this.baseUrl = baseUrl;
        this.endpointOverride = endpointOverride;
    }

    public Classification classify(String text, List<byte[]> images, String explicitSectionHint) throws IOException {
        return classify(text, images, explicitSectionHint, model, endpointOverride);
    }

    public Classification classify(String text, List<byte[]> images, String explicitSectionHint, String modelOverride, String endpointOverrideOverride) throws IOException {
        String prompt = PromptBuilder.build(text, explicitSectionHint);

        ObjectNode root = mapper.createObjectNode();
        root.put("model", modelOverride);

        ArrayNode messages = root.putArray("messages");
        ObjectNode system = messages.addObject();
        system.put("role", "system");
        system.put("content", "You are a strict classifier for a clothing resale catalog. Return only valid JSON.");

        ObjectNode user = messages.addObject();
        user.put("role", "user");
        ArrayNode content = user.putArray("content");
        ObjectNode textNode = content.addObject();
        textNode.put("type", "text");
        textNode.put("text", prompt);

        for (byte[] img : images) {
            ObjectNode imageNode = content.addObject();
            imageNode.put("type", "image_url");
            ObjectNode imageUrl = imageNode.putObject("image_url");
            imageUrl.put("url", "data:image/jpeg;base64," + Base64.getEncoder().encodeToString(img));
        }

        String body = mapper.writeValueAsString(root);
        Request request = new Request.Builder()
                .url(resolveEndpoint(modelOverride, endpointOverrideOverride))
                .addHeader("Authorization", "Bearer " + apiKey)
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(body, JSON))
                .build();

        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Kie AI error: " + response.code() + " " + response.message());
            }
            String payload = response.body() == null ? "" : response.body().string();
            return parseClassification(payload);
        }
    }

    private String resolveEndpoint(String modelOverride, String endpointOverrideOverride) {
        if (endpointOverrideOverride != null && !endpointOverrideOverride.isBlank()) {
            return endpointOverrideOverride;
        }
        String cleanBase = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        return cleanBase + "/" + modelOverride + "/v1/chat/completions";
    }

    private Classification parseClassification(String responseBody) throws IOException {
        JsonNode root = mapper.readTree(responseBody);
        JsonNode content = root.at("/choices/0/message/content");
        if (content.isMissingNode() || content.isNull()) {
            throw new IOException("Kie AI response missing content");
        }
        String text = content.asText();
        String json = extractJson(text);
        JsonNode node = mapper.readTree(json);

        Classification c = new Classification();
        c.title = node.path("title").asText("");
        c.description = node.path("description").asText("");
        c.size = node.path("size").asText("");
        c.priceEur = node.path("price_eur").asText("");
        c.priceRsd = node.path("price_rsd").asText("");

        if (node.has("tags") && node.get("tags").isArray()) {
            for (JsonNode t : node.get("tags")) {
                c.tags.add(t.asText());
            }
        }

        if (node.has("categories") && node.get("categories").isArray()) {
            for (JsonNode cat : node.get("categories")) {
                String section = cat.path("section").asText("");
                String sub = cat.path("subcategory").asText("");
                if (!section.isBlank()) {
                    c.categories.add(new CategorySelection.Entry(section, sub.isBlank() ? null : sub));
                }
            }
        }
        return c;
    }

    private String extractJson(String text) throws IOException {
        int start = text.indexOf('{');
        int end = text.lastIndexOf('}');
        if (start < 0 || end < start) {
            throw new IOException("No JSON object found in response");
        }
        return text.substring(start, end + 1);
    }
}
