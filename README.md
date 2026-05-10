# Olist E-commerce Data Warehouse ETL Project

Dự án này thực hiện quy trình ETL (Extract, Transform, Load) để xây dựng Data Warehouse từ dữ liệu thương mại điện tử Olist. Dữ liệu được trích xuất từ SQL Server, biến đổi và nạp vào PostgreSQL bằng Apache Airflow.

## 🚀 Công nghệ sử dụng
- **Orchestration:** Apache Airflow 3.1.8 (Chạy trên Docker)
- **Source Database:** Microsoft SQL Server
- **Target Data Warehouse:** PostgreSQL 16
- **Database Management:** pgAdmin 4
- **Language:** Python (Pandas, SQLAlchemy, PyODBC)

## 📁 Cấu trúc dự án
- `dags/`: Chứa mã nguồn của Airflow DAG.
    - `olist_etl_dag.py`: Định nghĩa pipeline chính.
    - `etl/`: Chứa các module xử lý Extract, Transform, Load và Validate.
- `datasets/`: Chứa các tệp dữ liệu CSV gốc (nếu có).
- `docker-compose.yaml`: Cấu hình Docker cho Airflow, Postgres và Redis.
- `Dockerfile`: Xây dựng Airflow image tùy chỉnh với các driver kết nối MSSQL.
- `Olist.sql`: Script khởi tạo cơ sở dữ liệu nguồn trên SQL Server.
- `.env`: Tệp cấu hình các biến môi trường và thông tin kết nối.

## 🛠️ Hướng dẫn cài đặt

### 1. Yêu cầu hệ thống
- Đã cài đặt **Docker** và **Docker Compose**.
- Đã cài đặt **Microsoft SQL Server** (làm database nguồn).

### 2. Cấu hình biến môi trường
Tạo tệp `.env` ở thư mục gốc (nếu chưa có) và chỉnh sửa các thông tin kết nối:
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

# Airflow UID (Dùng lệnh 'id -u' trên Linux/macOS)
AIRFLOW_UID=50000
```

### 3. Chuẩn bị Database nguồn (SQL Server)
Chạy script `Olist.sql` trong SQL Server Management Studio (SSMS) hoặc công cụ tương đương để tạo database `OlistDB_Final_2` và nạp dữ liệu từ các file CSV trong thư mục `datasets`.

### 4. Khởi chạy Airflow bằng Docker
Mở terminal tại thư mục dự án và chạy các lệnh sau:

**Khởi tạo Airflow (Chỉ chạy lần đầu):**
```bash
docker-compose up airflow-init
```

**Khởi động tất cả các dịch vụ:**
```bash
docker-compose up -d
```

## 📈 Cách vận hành dự án

### 1. Truy cập các giao diện quản trị
- **Airflow Web UI:** [http://localhost:8080](http://localhost:8080)
    - Username: `airflow`
    - Password: `airflow`
- **pgAdmin:** [http://localhost:5050](http://localhost:5050)
    - Email: `admin@admin.com`
    - Password: `admin`

### 2. Chạy Pipeline
1. Đăng nhập vào Airflow.
2. Tìm DAG có tên `olist_etl_pipeline`.
3. Bật (Unpause) DAG và nhấn **Trigger DAG** để bắt đầu quy trình ETL.

### 3. Quy trình ETL
- **Extract:** Trích xuất dữ liệu từ các bảng trong SQL Server.
- **Transform & Load (Dims):** Chuyển đổi và nạp dữ liệu vào các bảng Dimension (Date, Customer, Product, Seller, Geolocation, v.v.).
- **Transform & Load (Facts):** Nạp dữ liệu vào các bảng Fact (Sale, Delivery, Reviews).
- **Validate:** Kiểm tra tính toàn vẹn và số lượng bản ghi sau khi nạp.

## 📝 Lưu ý
- Nếu SQL Server chạy trực tiếp trên Windows, hãy giữ `MSSQL_HOST=host.docker.internal` trong tệp `.env`.
- Đảm bảo SQL Server cho phép kết nối từ xa (Remote Connections) và xác thực bằng tài khoản SQL.
- Dữ liệu Data Warehouse sẽ được lưu trữ trong container Postgres tại database `olist_dw`.
