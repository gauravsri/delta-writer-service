package com.example.deltastore.testdata;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Test-only Order entity for testing generic entity framework.
 * This class is used exclusively for testing and demonstrations.
 */
public class Order implements GenericRecord {
    
    private static final Schema SCHEMA = new Schema.Parser().parse("""
        {
            "type": "record",
            "name": "Order",
            "namespace": "com.example.deltastore.testdata",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "customer_id", "type": "string"},
                {"name": "product_id", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "unit_price", "type": "double"},
                {"name": "total_amount", "type": "double"},
                {"name": "order_date", "type": "string"},
                {"name": "region", "type": "string"},
                {"name": "status", "type": {"type": "enum", "name": "OrderStatus", "symbols": ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]}}
            ]
        }
        """);
    
    private final GenericData.Record record;
    
    public Order() {
        this.record = new GenericData.Record(SCHEMA);
    }
    
    public Order(String orderId, String customerId, String productId, int quantity, double unitPrice, String orderDate, String region, String status) {
        this();
        setOrderId(orderId);
        setCustomerId(customerId);
        setProductId(productId);
        setQuantity(quantity);
        setUnitPrice(unitPrice);
        setTotalAmount(quantity * unitPrice);
        setOrderDate(orderDate);
        setRegion(region);
        setStatus(status);
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
    public String getOrderId() {
        return (String) get("order_id");
    }
    
    public void setOrderId(String orderId) {
        put("order_id", orderId);
    }
    
    public String getCustomerId() {
        return (String) get("customer_id");
    }
    
    public void setCustomerId(String customerId) {
        put("customer_id", customerId);
    }
    
    public String getProductId() {
        return (String) get("product_id");
    }
    
    public void setProductId(String productId) {
        put("product_id", productId);
    }
    
    public int getQuantity() {
        return (Integer) get("quantity");
    }
    
    public void setQuantity(int quantity) {
        put("quantity", quantity);
    }
    
    public double getUnitPrice() {
        return (Double) get("unit_price");
    }
    
    public void setUnitPrice(double unitPrice) {
        put("unit_price", unitPrice);
    }
    
    public double getTotalAmount() {
        return (Double) get("total_amount");
    }
    
    public void setTotalAmount(double totalAmount) {
        put("total_amount", totalAmount);
    }
    
    public String getOrderDate() {
        return (String) get("order_date");
    }
    
    public void setOrderDate(String orderDate) {
        put("order_date", orderDate);
    }
    
    public String getRegion() {
        return (String) get("region");
    }
    
    public void setRegion(String region) {
        put("region", region);
    }
    
    public String getStatus() {
        return get("status").toString();
    }
    
    public void setStatus(String status) {
        put("status", new GenericData.EnumSymbol(SCHEMA.getField("status").schema(), status));
    }
    
    @Override
    public String toString() {
        return record.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Order order = (Order) obj;
        return record.equals(order.record);
    }
    
    @Override
    public int hashCode() {
        return record.hashCode();
    }
}