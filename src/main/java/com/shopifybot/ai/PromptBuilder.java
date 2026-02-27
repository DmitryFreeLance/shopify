package com.shopifybot.ai;

public class PromptBuilder {
    public static String build(String text, String explicitSectionHint) {
        StringBuilder sb = new StringBuilder();
        sb.append("You will receive a marketplace post text and product photos. ");
        sb.append("Your job: classify into catalog categories and extract listing data. ");
        sb.append("Always return valid JSON only, no markdown.\n\n");
        sb.append("Allowed sections: Muško, Žensko, Dečija kolekcija, Sniženje.\n");
        sb.append("Allowed subcategories: Jakne, Duksevi, Džemperi, Košulje, Majice, Farmerke, Pantalone, Donji deo trenerke, Gornji deo trenerke, Šorcevi.\n");
        sb.append("If the item can belong to multiple sections, return multiple category entries.\n");
        if (explicitSectionHint != null && !explicitSectionHint.isBlank()) {
            sb.append("Text hints section: ").append(explicitSectionHint).append(". Respect explicit section hints if present.\n");
        }
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
