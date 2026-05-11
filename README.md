# Olist E-commerce Data Warehouse ETL Project

Dự án thực hiện quy trình ETL (Extract, Transform, Load) để xây dựng Data Warehouse từ dữ liệu thương mại điện tử Olist Brazil. Dữ liệu được trích xuất từ SQL Server, biến đổi và nạp vào PostgreSQL bằng Apache Airflow.

## 🚀 Công nghệ sử dụng
- **Orchestration:** Apache Airflow (chạy trên Docker)
- **Source Database:** Microsoft SQL Server
- **Target Data Warehouse:** PostgreSQL 16
- **Database Management:** pgAdmin 4
- **Language:** Python (Pandas, SQLAlchemy, PyODBC)

## 📁 Cấu trúc dự án
```
OLIST/
├── dags/
│   ├── olist_etl_dag.py        # Định nghĩa pipeline chính
│   └── etl/
│       ├── config.py           # Cấu hình kết nối database
│       ├── extract.py          # Trích xuất dữ liệu từ SQL Server
│       ├── transform_dims.py   # Load các bảng Dimension
│       ├── transform_facts.py  # Load các bảng Fact
│       └── validate.py         # Kiểm tra tính toàn vẹn DW
├── datasets/                   # Tệp CSV gốc
├── docker-compose.yaml         # Cấu hình Docker
├── Dockerfile                  # Airflow image tùy chỉnh (có MSSQL driver)
├── Olist.sql                   # Script khởi tạo database nguồn trên SQL Server
└── .env                        # Biến môi trường và thông tin kết nối
```

## 🗄️ Thiết kế Data Warehouse (Star Schema)

### Bảng Dimension
| Bảng | Mô tả |
|---|---|
| `dim_date` | Chiều thời gian (2016–2018) |
| `dim_customer` | Khách hàng |
| `dim_seller` | Người bán (SCD Type 2) |
| `dim_product` | Sản phẩm (có category tiếng Anh, volume, size_bucket) |
| `dim_geolocation` | Địa lý theo mã zip code |
| `dim_paymenttype` | Phương thức thanh toán |
| `dim_orderstatus` | Trạng thái đơn hàng |
| `dim_order` | Đơn hàng (bảng mới — bridge giữa customer và các fact) |

### Bảng Fact
| Bảng | Grain | Mô tả |
|---|---|---|
| `fact_sale` | 1 order_item | Sự kiện bán hàng |
| `fact_payment` | 1 payment_sequential | Sự kiện thanh toán (tách riêng để đúng grain) |
| `fact_delivery` | 1 order đã giao | Sự kiện giao hàng (chỉ status = delivered) |
| `fact_reviews` | 1 review_id | Đánh giá của khách hàng |

## 🛠️ Hướng dẫn cài đặt

### 1. Yêu cầu hệ thống
- Đã cài đặt **Docker** và **Docker Compose**
- Đã cài đặt **Microsoft SQL Server** (làm database nguồn)

### 2. Cấu hình biến môi trường
Tạo tệp `.env` ở thư mục gốc và chỉnh sửa thông tin kết nối:
```env
# SQL Server (Source)
MSSQL_HOST=host.docker.internal
MSSQL_DB=OlistDB_Final_2
MSSQL_USER=sa
MSSQL_PASS=your_password

# PostgreSQL (Target DW)
PG_USER=airflow
PG_PASS=airflow
PG_HOST=postgres
PG_PORT=5432
PG_DB=olist_dw

# Airflow UID (chạy lệnh 'id -u' trên Linux/macOS để lấy)
AIRFLOW_UID=50000
```

### 3. Chuẩn bị Database nguồn (SQL Server)
Chạy script `Olist.sql` trong SQL Server Management Studio (SSMS) để tạo database `OlistDB_Final_2` và nạp dữ liệu từ các file CSV trong thư mục `datasets/`.

### 4. Khởi chạy Airflow bằng Docker
```bash
# Khởi tạo Airflow — chỉ chạy lần đầu
docker-compose up airflow-init

# Khởi động tất cả dịch vụ
docker-compose up -d
```

## 📈 Cách vận hành

### 1. Truy cập giao diện quản trị
| Dịch vụ | URL | Tài khoản |
|---|---|---|
| Airflow Web UI | http://localhost:8080 | `airflow` / `airflow` |
| pgAdmin | http://localhost:5050 | `admin@admin.com` / `admin` |

### 2. Chạy Pipeline
1. Đăng nhập vào Airflow Web UI
2. Tìm DAG `olist_etl_pipeline`
3. Bật (Unpause) DAG và nhấn **Trigger DAG ▶**

### 3. Thứ tự thực thi (Dependency Graph)
```
extract_from_sqlserver
    ├── dim_date
    ├── dim_geolocation
    │       ├── dim_customer
    │       └── dim_seller
    ├── dim_paymenttype
    ├── dim_orderstatus
    │       └── (cùng dim_customer) → dim_order
    └── dim_product
            └── (tất cả dim xong) ──┬── fact_sale
                                    ├── fact_payment
                                    ├── fact_delivery
                                    └── fact_reviews
                                            └── validate_dw
```

### 4. Chạy lại từ đầu (Reset DW)
```bash
# Xóa toàn bộ data DW
docker-compose exec postgres psql -U airflow -d olist_dw -c "DROP SCHEMA IF EXISTS dw CASCADE; CREATE SCHEMA dw;"

# Restart scheduler
docker-compose restart airflow-scheduler

# Trigger lại pipeline
docker-compose exec airflow-scheduler airflow dags trigger olist_etl_pipeline
```

## 📝 Lưu ý
- Nếu SQL Server chạy trực tiếp trên Windows, giữ `MSSQL_HOST=host.docker.internal` trong `.env`
- Đảm bảo SQL Server cho phép kết nối từ xa (Remote Connections) và xác thực bằng tài khoản SQL
- Data Warehouse được lưu trong schema `dw` của database `olist_dw` trên container Postgres
- `fact_payment` được tách riêng khỏi `fact_sale` để đảm bảo đúng grain (1 đơn có thể có nhiều payment_sequential)
- `dim_order` là bảng mới làm bridge, bắt buộc phải load trước tất cả các bảng Fact