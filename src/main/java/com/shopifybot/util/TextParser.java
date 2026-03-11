package com.shopifybot.util;

import com.shopifybot.ai.CategorySelection;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextParser {
    private static final Pattern EUR_PATTERN = Pattern.compile("(?i)(?:€\\s*|\\b)(\\d+[\\d.,]*)\\s*€");
    private static final Pattern EUR_PATTERN_ALT = Pattern.compile("(?i)€\\s*(\\d+[\\d.,]*)");
    private static final Pattern RSD_PATTERN = Pattern.compile("(?i)(?:=|rsd|din|дин)\\s*(\\d+[\\d.,]*)");
    private static final Pattern RSD_PATTERN_BEFORE = Pattern.compile("(?i)(\\d+[\\d.,]*)\\s*(rsd|din|дин)");
    private static final Pattern EUR_TEXT_PATTERN = Pattern.compile("(?i)(\\d+[\\d.,]*)\\s*eur");
    private static final Pattern CENA_PATTERN = Pattern.compile("(?iu)\\b(cena|цена)\\b\\s*[-:]*\\s*(\\d+[\\d.,]*)\\s*(rsd|din|дин|eur|€)?");
    private static final Pattern SIZE_PATTERN = Pattern.compile("(?i)(?:vel|veli[čc]ina|size)\\s*[-:]?\\s*([^\\n]+)");
    private static final Pattern DISCOUNT_PATTERN = Pattern.compile("(?i)(\\d+[\\d.,]*)\\s*[-–]\\s*(\\d{1,2})\\s*%\\s*=\\s*(\\d+[\\d.,]*)");
    private static final Pattern DISCOUNT_FRAGMENT_PATTERN = Pattern.compile("(?iu)^[\\d\\s.,%\\-–=€]+$");

    private static final List<String> MALE_HINTS = Arrays.asList("muški", "muski", "muško", "musko", "men", "male");
    private static final List<String> FEMALE_HINTS = Arrays.asList("ženski", "zenski", "žensko", "zensko", "women", "female");
    private static final List<String> CHILD_HINTS = Arrays.asList("dečija", "decija", "dečije", "decije", "kids", "child", "deca", "djeca");
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

    public static boolean containsSaleKeyword(String text) {
        return containsAnyKeyword(text, SALE_HINTS);
    }

    public static boolean containsSnizenje(String text) {
        if (text == null) return false;
        String lower = text.toLowerCase(Locale.ROOT);
        return lower.contains("sniženje") || lower.contains("snizenje") || lower.contains("снижение");
    }

    public static String extractTitle(String text) {
        if (text == null || text.isBlank()) return "";
        String[] lines = text.split("\n");
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            String lower = line.toLowerCase(Locale.ROOT);
            if (startsWithSaleHeaderLine(lower)) {
                continue;
            }
            if (DISCOUNT_PATTERN.matcher(line).find()) {
                continue;
            }
            if (EUR_PATTERN.matcher(line).find() || EUR_PATTERN_ALT.matcher(line).find() || EUR_TEXT_PATTERN.matcher(line).find()) {
                continue;
            }
            if (RSD_PATTERN.matcher(line).find() || RSD_PATTERN_BEFORE.matcher(line).find()) {
                continue;
            }
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
        m = EUR_TEXT_PATTERN.matcher(text);
        if (m.find()) return normalizeNumber(m.group(1));
        return "";
    }

    public static String extractRsd(String text) {
        if (text == null) return "";
        Matcher m = RSD_PATTERN.matcher(text);
        if (m.find()) return normalizeNumber(m.group(1));
        m = RSD_PATTERN_BEFORE.matcher(text);
        if (m.find()) return normalizeNumber(m.group(1));
        m = CENA_PATTERN.matcher(text);
        if (m.find()) {
            String currency = m.group(3);
            if (currency == null || currency.isBlank()) {
                return normalizeNumber(m.group(2));
            }
            String lower = currency.toLowerCase(Locale.ROOT);
            if (lower.contains("rsd") || lower.contains("din") || lower.contains("дин")) {
                return normalizeNumber(m.group(2));
            }
        }
        return "";
    }

    public static DiscountInfo extractDiscount(String text) {
        if (text == null) return null;
        Matcher m = DISCOUNT_PATTERN.matcher(text);
        if (!m.find()) return null;
        String original = normalizeNumber(m.group(1));
        String percent = m.group(2);
        String finalPrice = normalizeNumber(m.group(3));
        if (finalPrice == null || finalPrice.isBlank()) return null;
        return new DiscountInfo(original, percent, finalPrice);
    }

    public static boolean startsWithSaleHeader(String text) {
        if (text == null || text.isBlank()) return false;
        String[] lines = text.split("\\n");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;
            String lower = trimmed.toLowerCase(Locale.ROOT);
            return startsWithSaleHeaderLine(lower);
        }
        return false;
    }

    private static boolean startsWithSaleHeaderLine(String lowerLine) {
        return lowerLine.startsWith("sniženje") || lowerLine.startsWith("snizenje");
    }

    private static String normalizeNumber(String raw) {
        String cleaned = raw.replaceAll("[^0-9.,]", "");
        if (cleaned.matches("\\d{1,3}(\\.\\d{3})+")) {
            return cleaned.replace(".", "");
        }
        if (cleaned.matches("\\d{1,3}(,\\d{3})+")) {
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
        boolean sale = containsAny(lower, SALE_HINTS);

        CategorySelection selection = new CategorySelection();

        if (sale) selection.add("Sniženje", null);
        if (male) selection.add("Muško", null);
        if (female) selection.add("Žensko", null);

        String sub = detectSubcategory(lower);
        // Subcategories are not used in the current catalog.
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

    public static String removeLinesStartingWith(String text, List<String> prefixes) {
        if (text == null || text.isBlank()) return text;
        String[] lines = text.split("\\n");
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;
            String lower = trimmed.toLowerCase(Locale.ROOT);
            boolean skip = false;
            for (String prefix : prefixes) {
                if (lower.startsWith(prefix.toLowerCase(Locale.ROOT))) {
                    skip = true;
                    break;
                }
            }
            if (skip) continue;
            if (sb.length() > 0) sb.append("\n");
            sb.append(trimmed);
        }
        return sb.toString();
    }

    public static String normalizeSaleLines(String text) {
        if (text == null || text.isBlank()) return text;
        String[] lines = text.split("\\n");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;
            String lower = trimmed.toLowerCase(Locale.ROOT);
            if (startsWithSaleHeaderLine(lower)) {
                StringBuilder sale = new StringBuilder(trimmed);
                int j = i + 1;
                while (j < lines.length) {
                    String nextTrimmed = lines[j].trim();
                    if (nextTrimmed.isEmpty()) {
                        j++;
                        continue;
                    }
                    if (isDiscountFragment(nextTrimmed) || DISCOUNT_PATTERN.matcher(nextTrimmed).find()) {
                        sale.append(" ").append(nextTrimmed);
                        j++;
                        continue;
                    }
                    break;
                }
                String compact = sale.toString().replaceAll("\\s+", " ").trim();
                if (sb.length() > 0) sb.append("\n");
                sb.append(compact);
                i = j - 1;
                continue;
            }
            if (sb.length() > 0) sb.append("\n");
            sb.append(trimmed);
        }
        return sb.toString();
    }

    private static boolean isDiscountFragment(String value) {
        if (value == null || value.isBlank()) return false;
        String trimmed = value.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (lower.equals("rsd") || lower.equals("din") || lower.equals("дин") || lower.equals("eur") || lower.equals("€")) {
            return true;
        }
        return DISCOUNT_FRAGMENT_PATTERN.matcher(trimmed).matches();
    }

    public static class DiscountInfo {
        public final String original;
        public final String percent;
        public final String finalPrice;

        public DiscountInfo(String original, String percent, String finalPrice) {
            this.original = original;
            this.percent = percent;
            this.finalPrice = finalPrice;
        }
    }
}
