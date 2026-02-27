package com.shopifybot.ai;

import java.util.ArrayList;
import java.util.List;

public class CategorySelection {
    public final List<Entry> entries = new ArrayList<>();

    public void add(String section, String subcategory) {
        entries.add(new Entry(section, subcategory));
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public static class Entry {
        public String section;
        public String subcategory;

        public Entry(String section, String subcategory) {
            this.section = section;
            this.subcategory = subcategory;
        }
    }
}
