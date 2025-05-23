--SQL Schema
CREATE DATABASE product_data;
USE product_data;

CREATE TABLE products (
    sku_id BIGINT PRIMARY KEY,
    product_id BIGINT,
    availability VARCHAR(50),
    venture_category2_name_en VARCHAR(255),
    venture_category1_name_en VARCHAR(255),
    brand_name VARCHAR(255),
    venture_category_name_local VARCHAR(255),
    seller_name VARCHAR(255),
    business_area VARCHAR(255),
    business_type VARCHAR(255),
    product_name VARCHAR(255),
    product_url TEXT,
    description TEXT,
    platform_commission_rate DECIMAL(5,2) DEFAULT 0,
    is_free_shipping BOOLEAN,
    product_commission_rate DECIMAL(5,2),
    current_price DECIMAL(10,2),
    price DECIMAL(10,2),
    rating_avg_value DECIMAL(3,1),
    seller_rating DECIMAL(3,1),
    bonus_commission_rate DECIMAL(5,2),
    discount_percentage DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better query performance
CREATE INDEX idx_product_id ON products(product_id);
CREATE INDEX idx_brand_name ON products(brand_name);
CREATE INDEX idx_seller_name ON products(seller_name);
CREATE INDEX idx_business_type ON products(business_type);
CREATE INDEX idx_current_price ON products(current_price);
CREATE INDEX idx_rating ON products(rating_avg_value);
