package com.shopifybot.ai;

public class PromptBuilder {
    public static String build(String text, String explicitSectionHint) {
        StringBuilder sb = new StringBuilder();
        sb.append("You will receive a marketplace post text and product photos. ");
        sb.append("Your job: classify into catalog categories and extract listing data. ");
        sb.append("Always return valid JSON only, no markdown.\n\n");
        sb.append("Allowed sections: Muško, Žensko, Sniženje.\n");
        sb.append("If the item can belong to multiple sections, return multiple category entries.\n");
        if (explicitSectionHint != null && !explicitSectionHint.isBlank()) {
            sb.append("Text hints section: ").append(explicitSectionHint).append(". Respect explicit section hints if present.\n");
        }
        sb.append("Price extraction rules are strict:\n");
        sb.append("1. If any image contains a price tag with the text 'RSD', the RSD price must be taken from the number printed immediately above, next to, or otherwise directly attached to 'RSD'.\n");
        sb.append("2. Mentally rotate the photo if needed and read the price tag correctly before extracting the price.\n");
        sb.append("3. Barcode digits, SKU/article digits, and any long number near a barcode are never the price, even if they visually resemble a price.\n");
        sb.append("4. If both a barcode/article number and a smaller number near 'RSD' exist on the same tag, always choose the number tied to 'RSD'.\n");
        sb.append("5. For Serbian listings, prefer the hanging/tag price in RSD over any other numeric text in the photos.\n");
        sb.append("Return JSON with this shape:\n");
        sb.append("{\n");
        sb.append("  \"title\": string,\n");
        sb.append("  \"description\": string,\n");
        sb.append("  \"size\": string,\n");
        sb.append("  \"price_eur\": string,\n");
        sb.append("  \"price_rsd\": string,\n");
        sb.append("  \"tags\": [string],\n");
        sb.append("  \"categories\": [{\"section\": string, \"subcategory\": string|null}]\n");
        sb.append("}\n\n");
        sb.append("Post text:\n");
        sb.append(text == null ? "" : text);
        return sb.toString();
    }
}
