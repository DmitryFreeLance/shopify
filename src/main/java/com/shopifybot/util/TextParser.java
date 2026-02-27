package com.shopifybot.util;

import com.shopifybot.ai.CategorySelection;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextParser {
    private static final Pattern EUR_PATTERN = Pattern.compile("(?i)(?:€\s*|\b)(\d+[\d.,]*)\s*€");
    private static final Pattern EUR_PATTERN_ALT = Pattern.compile("(?i)€\s*(\d+[\d.,]*)");
    private static final Pattern RSD_PATTERN = Pattern.compile("(?i)(?:=|rsd|din|дин)\s*(\d+[\d.,]*)");
    private static final Pattern SIZE_PATTERN = Pattern.compile("(?i)(?:vel|veli[čc]ina|size)\s*[-:]?\s*([^\n]+)");

    private static final List<String> MALE_HINTS = Arrays.asList("muški", "muski", "muško", "musko", "men", "male");
    private static final List<String> FEMALE_HINTS = Arrays.asList("ženski", "zenski", "žensko", "zensko", "women", "female");
    private static final List<String> CHILD_HINTS = Arrays.asList("dečija", "decija", "kids", "child", "deca", "djeca");
    private static final List<String> SALE_HINTS = Arrays.asList("sniženje", "snizenje", "sale", "akcija", "popust", "discount");

    private static final Map<String, String> SUBCATEGORY_MAP = new LinkedHashMap<>();
    static {
        SUBCATEGORY_MAP.put("jakne", "Jakne");
        SUBCATEGORY_MAP.put("duks", "Duksevi");
        SUBCATEGORY_MAP.put("duksevi", "Duksevi");
        SUBCATEGORY_MAP.put("džemper", "Džemperi");
        SUBCATEGORY_MAP.put("dzemper", "Džemperi");
        SUBCATEGORY_MAP.put("košulj", "Košulje");
        SUBCATEGORY_MAP.put("kosulj", "Košulje");
        SUBCATEGORY_MAP.put("majic", "Majice");
        SUBCATEGORY_MAP.put("farmerk", "Farmerke");
        SUBCATEGORY_MAP.put("pantalon", "Pantalone");
        SUBCATEGORY_MAP.put("donji deo trenerke", "Donji deo trenerke");
        SUBCATEGORY_MAP.put("gornji deo trenerke", "Gornji deo trenerke");
        SUBCATEGORY_MAP.put("šorcev", "Šorcevi");
        SUBCATEGORY_MAP.put("sorcev", "Šorcevi");
        SUBCATEGORY_MAP.put("šorc", "Šorcevi");
        SUBCATEGORY_MAP.put("sorc", "Šorcevi");
    }

    public static boolean containsKeyword(String text, String keyword) {
        if (text == null) return false;
        return text.toLowerCase(Locale.ROOT).contains(keyword.toLowerCase(Locale.ROOT));
    }

    public static boolean containsAnyKeyword(String text, List<String> keywords) {
        if (text == null) return false;
        String lower = text.toLowerCase(Locale.ROOT);
        for (String keyword : keywords) {
            if (lower.contains(keyword.toLowerCase(Locale.ROOT))) return true;
        }
        return false;
    }

    public static String extractTitle(String text) {
        if (text == null || text.isBlank()) return "";
        String[] lines = text.split("\n");
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.contains("cena") || lower.contains("vel") || lower.contains("veli")) {
                break;
            }
            if (!line.isBlank()) {
                if (sb.length() > 0) sb.append(" ");
                sb.append(line.trim());
            }
        }
        return sb.toString().trim();
    }

    public static String extractSize(String text) {
        if (text == null) return "";
        Matcher m = SIZE_PATTERN.matcher(text);
        if (m.find()) {
            return m.group(1).trim();
        }
        return "";
    }

    public static String extractEur(String text) {
        if (text == null) return "";
        Matcher m = EUR_PATTERN.matcher(text);
        if (m.find()) return normalizeNumber(m.group(1));
        m = EUR_PATTERN_ALT.matcher(text);
        if (m.find()) return normalizeNumber(m.group(1));
        return "";
    }

    public static String extractRsd(String text) {
        if (text == null) return "";
        Matcher m = RSD_PATTERN.matcher(text);
        if (m.find()) return normalizeNumber(m.group(1));
        return "";
    }

    private static String normalizeNumber(String raw) {
        String cleaned = raw.replaceAll("[^0-9.,]", "");
        if (cleaned.matches("\\\\d{1,3}(\\\\.\\\\d{3})+")) {
            return cleaned.replace(".", "");
        }
        if (cleaned.matches("\\\\d{1,3}(,\\\\d{3})+")) {
            return cleaned.replace(",", "");
        }
        if (cleaned.contains(",") && cleaned.contains(".")) {
            cleaned = cleaned.replace(".", "");
            cleaned = cleaned.replace(",", ".");
            return cleaned;
        }
        if (cleaned.contains(",")) {
            return cleaned.replace(",", ".");
        }
        return cleaned;
    }

    public static CategorySelection detectExplicitCategories(String text) {
        if (text == null) return new CategorySelection();
        String lower = text.toLowerCase(Locale.ROOT);

        boolean male = containsAny(lower, MALE_HINTS) || lower.contains("mušk") || lower.contains("musko");
        boolean female = containsAny(lower, FEMALE_HINTS) || lower.contains("žensk") || lower.contains("zensko");
        boolean child = containsAny(lower, CHILD_HINTS);
        boolean sale = containsAny(lower, SALE_HINTS);

        CategorySelection selection = new CategorySelection();

        if (child) selection.add("Dečija kolekcija", null);
        if (sale) selection.add("Sniženje", null);
        if (male) selection.add("Muško", null);
        if (female) selection.add("Žensko", null);

        String sub = detectSubcategory(lower);
        if (sub != null) {
            if (selection.entries.isEmpty()) {
                selection.add("Muško", sub);
            } else {
                selection.entries.forEach(e -> {
                    if ("Muško".equals(e.section) || "Žensko".equals(e.section)) {
                        e.subcategory = sub;
                    }
                });
            }
        }
        return selection;
    }

    private static String detectSubcategory(String lower) {
        for (Map.Entry<String, String> entry : SUBCATEGORY_MAP.entrySet()) {
            if (lower.contains(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static boolean containsAny(String lower, List<String> keywords) {
        for (String keyword : keywords) {
            if (lower.contains(keyword)) return true;
        }
        return false;
    }
}
