package com.shopifybot.ai;

import java.util.ArrayList;
import java.util.List;

public class Classification {
    public String title;
    public String description;
    public String size;
    public String priceEur;
    public String priceRsd;
    public List<String> tags = new ArrayList<>();
    public List<CategorySelection.Entry> categories = new ArrayList<>();
}
