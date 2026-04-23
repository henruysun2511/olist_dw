-- ============================================================
-- 1. TẠO DATABASE MỚI
-- ============================================================
USE master;
GO
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'OlistDB_Final')
    DROP DATABASE OlistDB_Final;
GO
CREATE DATABASE OlistDB_Final_2;
GO
USE OlistDB_Final_2;
GO

-- ============================================================
-- 2. KHỞI TẠO CẤU TRÚC BẢNG (Nới rộng độ dài để nạp an toàn)
-- ============================================================

CREATE TABLE olist_customers (
    customer_id VARCHAR(100) NOT NULL,
    customer_unique_id VARCHAR(100) NOT NULL,
    customer_zip_code_prefix VARCHAR(20),
    customer_city NVARCHAR(255),
    customer_state NVARCHAR(50),
    CONSTRAINT PK_customers PRIMARY KEY (customer_id)
);

CREATE TABLE olist_sellers (
    seller_id VARCHAR(100) NOT NULL,
    seller_zip_code_prefix VARCHAR(20),
    seller_city NVARCHAR(255),
    seller_state NVARCHAR(50),
    CONSTRAINT PK_sellers PRIMARY KEY (seller_id)
);

CREATE TABLE olist_products (
    product_id VARCHAR(100) NOT NULL,
    product_category_name NVARCHAR(255),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
    CONSTRAINT PK_products PRIMARY KEY (product_id)
);
select * from olist_products
CREATE TABLE product_category_translation (
    product_category_name NVARCHAR(255) NOT NULL,
    product_category_name_english NVARCHAR(255),
    CONSTRAINT PK_category_translation PRIMARY KEY (product_category_name)
);

CREATE TABLE olist_geolocation (
    geolocation_zip_code_prefix VARCHAR(20),
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city NVARCHAR(255),
    geolocation_state NVARCHAR(50)
);

CREATE TABLE olist_orders (
    order_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME2,
    order_approved_at DATETIME2,
    order_delivered_carrier_date DATETIME2,
    order_delivered_customer_date DATETIME2,
    order_estimated_delivery_date DATETIME2,
    CONSTRAINT PK_orders PRIMARY KEY (order_id)
);

CREATE TABLE olist_order_items (
    order_id VARCHAR(100) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(100),
    seller_id VARCHAR(100),
    shipping_limit_date DATETIME2,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    CONSTRAINT PK_order_items PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE olist_order_payments (
    order_id VARCHAR(100) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2),
    CONSTRAINT PK_payments PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE olist_order_reviews (
    review_id VARCHAR(100) NOT NULL,
    order_id VARCHAR(100),
    review_score INT,
    review_comment_title NVARCHAR(MAX),
    review_comment_message NVARCHAR(MAX),
    review_creation_date DATETIME2,
    review_answer_timestamp DATETIME2,
    CONSTRAINT PK_reviews PRIMARY KEY (review_id)
);

-- ============================================================
-- 3. NẠP DỮ LIỆU TỪ CSV (Dùng 0x0d0a cho CRLF)
-- ============================================================

PRINT 'Starting Bulk Insert...';

-- Định nghĩa các tham số nạp chung
-- Thay đổi ROWTERMINATOR thành '0x0d0a' (CRLF) hoặc '0x0a' (LF) tùy tệp
-- Thêm MAXERRORS = 100 để không bị dừng nếu có vài dòng lỗi định dạng

BULK INSERT product_category_translation FROM 'F:\DW\Olist\datasets\product_category_name_translation.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', MAXERRORS = 100);
BULK INSERT olist_geolocation FROM 'F:\DW\Olist\datasets\olist_geolocation_dataset.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', MAXERRORS = 100);
BULK INSERT olist_customers FROM 'F:\DW\Olist\datasets\olist_customers_dataset.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', MAXERRORS = 100);
BULK INSERT olist_sellers FROM 'F:\DW\Olist\datasets\olist_sellers_dataset.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', MAXERRORS = 100);
BULK INSERT olist_products FROM 'F:\DW\Olist\datasets\olist_products_dataset.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', MAXERRORS = 100);
BULK INSERT olist_orders FROM 'F:\DW\Olist\datasets\olist_orders_dataset.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', MAXERRORS = 100);
BULK INSERT olist_order_items FROM 'F:\DW\Olist\datasets\olist_order_items_dataset.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', MAXERRORS = 100);
BULK INSERT olist_order_payments FROM 'F:\DW\Olist\datasets\olist_order_payments_dataset.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', MAXERRORS = 100);
BULK INSERT olist_order_reviews FROM 'F:\DW\Olist\datasets\olist_order_reviews_dataset.csv' WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', MAXERRORS = 100);

-- GHI CHÚ: Nếu vẫn lỗi Truncation, hãy thay ROWTERMINATOR = '0x0a' thành '0x0d0a' cho các lệnh trên.

-- ============================================================
-- 4. CHUẨN HÓA GEOLOCATION
-- ============================================================

DELETE FROM olist_geolocation WHERE geolocation_zip_code_prefix IS NULL;

WITH CTE AS (
    SELECT geolocation_zip_code_prefix, 
    ROW_NUMBER() OVER (PARTITION BY geolocation_zip_code_prefix ORDER BY geolocation_zip_code_prefix) as rn
    FROM olist_geolocation
)
DELETE FROM CTE WHERE rn > 1;

ALTER TABLE olist_geolocation ALTER COLUMN geolocation_zip_code_prefix VARCHAR(20) NOT NULL;
ALTER TABLE olist_geolocation ADD CONSTRAINT PK_geolocation PRIMARY KEY (geolocation_zip_code_prefix);

-- ============================================================
-- 5. THIẾT LẬP KHÓA NGOẠI (Dùng WITH NOCHECK)
-- ============================================================

ALTER TABLE olist_orders WITH NOCHECK ADD CONSTRAINT FK_orders_customers FOREIGN KEY (customer_id) REFERENCES olist_customers(customer_id);
ALTER TABLE olist_order_items WITH NOCHECK ADD CONSTRAINT FK_items_orders FOREIGN KEY (order_id) REFERENCES olist_orders(order_id);
ALTER TABLE olist_order_items WITH NOCHECK ADD CONSTRAINT FK_items_products FOREIGN KEY (product_id) REFERENCES olist_products(product_id);
ALTER TABLE olist_order_items WITH NOCHECK ADD CONSTRAINT FK_items_sellers FOREIGN KEY (seller_id) REFERENCES olist_sellers(seller_id);
ALTER TABLE olist_order_payments WITH NOCHECK ADD CONSTRAINT FK_payments_orders FOREIGN KEY (order_id) REFERENCES olist_orders(order_id);
ALTER TABLE olist_order_reviews WITH NOCHECK ADD CONSTRAINT FK_reviews_orders FOREIGN KEY (order_id) REFERENCES olist_orders(order_id);
ALTER TABLE olist_products WITH NOCHECK ADD CONSTRAINT FK_products_translation FOREIGN KEY (product_category_name) REFERENCES product_category_translation(product_category_name);
ALTER TABLE olist_customers WITH NOCHECK ADD CONSTRAINT FK_customers_geo FOREIGN KEY (customer_zip_code_prefix) REFERENCES olist_geolocation(geolocation_zip_code_prefix);
ALTER TABLE olist_sellers WITH NOCHECK ADD CONSTRAINT FK_sellers_geo FOREIGN KEY (seller_zip_code_prefix) REFERENCES olist_geolocation(geolocation_zip_code_prefix);

PRINT 'Database setup completed successfully!';


CREATE TABLE staging_order_reviews (
    review_id NVARCHAR(MAX),
    order_id NVARCHAR(MAX),
    review_score NVARCHAR(MAX),
    review_comment_title NVARCHAR(MAX),
    review_comment_message NVARCHAR(MAX),
    review_creation_date NVARCHAR(MAX),
    review_answer_timestamp NVARCHAR(MAX)
);

-- Bước 2: Nạp dữ liệu vào bảng tạm (Dùng 0x0a và cho phép sai số)
BULK INSERT staging_order_reviews
FROM 'F:\DW\Olist\datasets\olist_order_reviews_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001',
    MAXERRORS = 1000 -- Cho phép bỏ qua các dòng bị lỗi format quá nặng
);

-- Bước 3: Đẩy dữ liệu từ bảng tạm sang bảng chính (Chỉ lấy dòng hợp lệ)
INSERT INTO olist_order_reviews
SELECT 
    CAST(LEFT(review_id, 32) AS VARCHAR(32)),
    CAST(LEFT(order_id, 32) AS VARCHAR(32)),
    TRY_CAST(review_score AS INT),
    review_comment_title,
    review_comment_message,
    TRY_CAST(review_creation_date AS DATETIME2),
    TRY_CAST(review_answer_timestamp AS DATETIME2)
FROM staging_order_reviews
WHERE TRY_CAST(review_score AS INT) IS NOT NULL; -- Lọc bỏ dòng tiêu đề hoặc dòng lệch cột

-- Xóa bảng tạm sau khi xong
DROP TABLE staging_order_reviews;

DROP TABLE IF EXISTS staging_order_reviews;

-- 2. Tạo bảng tạm với tất cả các cột là NVARCHAR(MAX) để "hứng" mọi loại dữ liệu rác
CREATE TABLE staging_order_reviews (
    review_id NVARCHAR(MAX),
    order_id NVARCHAR(MAX),
    review_score NVARCHAR(MAX),
    review_comment_title NVARCHAR(MAX),
    review_comment_message NVARCHAR(MAX),
    review_creation_date NVARCHAR(MAX),
    review_answer_timestamp NVARCHAR(MAX)
);

-- 3. Nạp dữ liệu thô vào bảng tạm
-- Dùng ROWTERMINATOR = '0x0a' (LF) và tăng MAXERRORS lên cao
BULK INSERT staging_order_reviews
FROM 'F:\DW\Olist\datasets\olist_order_reviews_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001',
    MAXERRORS = 1000 
);

-- 4. Đẩy dữ liệu từ bảng tạm sang bảng chính, đồng thời lọc trùng và ép kiểu
WITH CleanedReviews AS (
    SELECT 
        TRY_CAST(LEFT(review_id, 32) AS VARCHAR(32)) as r_id,
        TRY_CAST(LEFT(order_id, 32) AS VARCHAR(32)) as o_id,
        TRY_CAST(review_score AS INT) as r_score,
        review_comment_title,
        review_comment_message,
        TRY_CAST(review_creation_date AS DATETIME2) as r_create,
        TRY_CAST(review_answer_timestamp AS DATETIME2) as r_ans,
        ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_answer_timestamp DESC) as rn
    FROM staging_order_reviews
)
INSERT INTO olist_order_reviews (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
SELECT r_id, o_id, r_score, review_comment_title, review_comment_message, r_create, r_ans
FROM CleanedReviews
WHERE rn = 1               -- Chỉ lấy 1 bản ghi nếu trùng ID
  AND r_id IS NOT NULL     -- Loại bỏ dòng trắng
  AND r_score IS NOT NULL; -- Đảm bảo dòng đó được đọc đúng cột

-- 5. Xóa bảng tạm để sạch database
DROP TABLE staging_order_reviews;

PRINT 'Nạp và làm sạch bảng Reviews hoàn tất!';


DELETE FROM olist_geolocation 
WHERE geolocation_zip_code_prefix IS NULL;
GO

ALTER TABLE olist_geolocation 
ALTER COLUMN geolocation_zip_code_prefix VARCHAR(20) NOT NULL;
GO

WITH CTE AS (
    SELECT geolocation_zip_code_prefix, 
           ROW_NUMBER() OVER (PARTITION BY geolocation_zip_code_prefix ORDER BY geolocation_zip_code_prefix) as rn
    FROM olist_geolocation
)
DELETE FROM CTE WHERE rn > 1;
GO

-- Tạo Khóa chính
ALTER TABLE olist_geolocation 
ADD CONSTRAINT PK_geolocation PRIMARY KEY (geolocation_zip_code_prefix);
GO

-- Nối dây với bảng Customers (Dùng WITH NOCHECK để tránh lỗi dữ liệu không khớp)
ALTER TABLE olist_customers WITH NOCHECK 
ADD CONSTRAINT FK_customers_geo 
FOREIGN KEY (customer_zip_code_prefix) REFERENCES olist_geolocation(geolocation_zip_code_prefix);

-- Nối dây với bảng Sellers
ALTER TABLE olist_sellers WITH NOCHECK 
ADD CONSTRAINT FK_sellers_geo 
FOREIGN KEY (seller_zip_code_prefix) REFERENCES olist_geolocation(geolocation_zip_code_prefix);
GO