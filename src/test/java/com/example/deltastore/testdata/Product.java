package com.example.deltastore.testdata;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Test-only Product entity for testing generic entity framework.
 * This class is used exclusively for testing and demonstrations.
 */
public class Product implements GenericRecord {
    
    private static final Schema SCHEMA = new Schema.Parser().parse("""
        {
            "type": "record",
            "name": "Product",
            "namespace": "com.example.deltastore.testdata",
            "fields": [
                {"name": "product_id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "description", "type": ["null", "string"], "default": null},
                {"name": "category", "type": "string"},
                {"name": "subcategory", "type": ["null", "string"], "default": null},
                {"name": "brand", "type": ["null", "string"], "default": null},
                {"name": "price", "type": "double"},
                {"name": "currency", "type": "string", "default": "USD"},
                {"name": "stock_quantity", "type": "int"},
                {"name": "weight_kg", "type": ["null", "double"], "default": null},
                {"name": "dimensions", "type": ["null", "string"], "default": null},
                {"name": "is_active", "type": "boolean", "default": true},
                {"name": "created_date", "type": "string"},
                {"name": "last_updated", "type": ["null", "string"], "default": null}
            ]
        }
        """);
    
    private final GenericData.Record record;
    
    public Product() {
        this.record = new GenericData.Record(SCHEMA);
        // Set defaults
        setCurrency("USD");
        setIsActive(true);
    }
    
    public Product(String productId, String name, String category, double price, int stockQuantity, String createdDate) {
        this();
        setProductId(productId);
        setName(name);
        setCategory(category);
        setPrice(price);
        setStockQuantity(stockQuantity);
        setCreatedDate(createdDate);
    }
    
    @Override
    public Schema getSchema() {
        return SCHEMA;
    }
    
    public static Schema getClassSchema() {
        return SCHEMA;
    }
    
    @Override
    public void put(String key, Object v) {
        record.put(key, v);
    }
    
    @Override
    public Object get(String key) {
        return record.get(key);
    }
    
    @Override
    public void put(int i, Object v) {
        record.put(i, v);
    }
    
    @Override
    public Object get(int i) {
        return record.get(i);
    }
    
    // Convenience getters and setters
    public String getProductId() {
        return (String) get("product_id");
    }
    
    public void setProductId(String productId) {
        put("product_id", productId);
    }
    
    public String getName() {
        return (String) get("name");
    }
    
    public void setName(String name) {
        put("name", name);
    }
    
    public String getDescription() {
        return (String) get("description");
    }
    
    public void setDescription(String description) {
        put("description", description);
    }
    
    public String getCategory() {
        return (String) get("category");
    }
    
    public void setCategory(String category) {
        put("category", category);
    }
    
    public String getSubcategory() {
        return (String) get("subcategory");
    }
    
    public void setSubcategory(String subcategory) {
        put("subcategory", subcategory);
    }
    
    public String getBrand() {
        return (String) get("brand");
    }
    
    public void setBrand(String brand) {
        put("brand", brand);
    }
    
    public double getPrice() {
        return (Double) get("price");
    }
    
    public void setPrice(double price) {
        put("price", price);
    }
    
    public String getCurrency() {
        return (String) get("currency");
    }
    
    public void setCurrency(String currency) {
        put("currency", currency);
    }
    
    public int getStockQuantity() {
        return (Integer) get("stock_quantity");
    }
    
    public void setStockQuantity(int stockQuantity) {
        put("stock_quantity", stockQuantity);
    }
    
    public Double getWeightKg() {
        return (Double) get("weight_kg");
    }
    
    public void setWeightKg(Double weightKg) {
        put("weight_kg", weightKg);
    }
    
    public String getDimensions() {
        return (String) get("dimensions");
    }
    
    public void setDimensions(String dimensions) {
        put("dimensions", dimensions);
    }
    
    public boolean isActive() {
        return (Boolean) get("is_active");
    }
    
    public void setIsActive(boolean isActive) {
        put("is_active", isActive);
    }
    
    public String getCreatedDate() {
        return (String) get("created_date");
    }
    
    public void setCreatedDate(String createdDate) {
        put("created_date", createdDate);
    }
    
    public String getLastUpdated() {
        return (String) get("last_updated");
    }
    
    public void setLastUpdated(String lastUpdated) {
        put("last_updated", lastUpdated);
    }
    
    @Override
    public String toString() {
        return record.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Product product = (Product) obj;
        return record.equals(product.record);
    }
    
    @Override
    public int hashCode() {
        return record.hashCode();
    }
}