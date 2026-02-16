# Indonesia Air Quality & Weather Monitoring System

Sistem pemantauan kualitas udara dan prakiraan cuaca otomatis untuk kota-kota besar di Indonesia menggunakan arsitektur Modern Data Stack.

---

## 1. Project Overview & Problem Statement

### **Problem**

Kualitas udara dan cuaca di Indonesia belakangan ini menjadi sangat fluktuatif. Munculnya fenomena cuaca ekstrem seperti banjir, tanah longsor, hingga polusi udara yang tinggi memerlukan sistem pemantauan yang andal dan berkelanjutan.

### **Goals**

Project ini bertujuan untuk membangun pipeline data otomatis guna memonitor **Air Quality Index (AQI)** dan **Weather Forecast** secara _bathcing_ dari source data beberapa wilayah Indonesia.

### **Data Source**

Data diperoleh melalui metode _web scraping_ dari [IQAir Indonesia](https://www.iqair.com/indonesia).

---

## 🛠️ 2. Tech Stack & Environment

Project ini berjalan sepenuhnya di atas **Docker** untuk memastikan skalabilitas dan kemudahan replikasi environment lokal.

| Komponen           | Teknologi          | Peran / Deskripsi                                           |
| :----------------- | :----------------- | :---------------------------------------------------------- |
| **Orchestrator**   | **Apache Airflow** | Menjalankan scheduling pipeline (Bronze, Silver, Gold).     |
| **Query Engine**   | **Trino**          | Mesin SQL untuk memproses data dari Data Lake secara cepat. |
| **Object Storage** | **Minio**          | Storage utama (S3-compatible) untuk menyimpan file data.    |
| **Data Catalog**   | **Hive Metastore** | Pengelola metadata tabel untuk integrasi Trino & Iceberg.   |
| **Table Format**   | **Apache Iceberg** | Mendukung ACID transaksi dan _time travel_ pada Data Lake.  |
| **BI Tools**       | **Metabase**       | Visualisasi dashboard dan monitoring kualitas udara.        |
| **Database**       | **Postgres**       | Database metadata untuk Airflow, Hive, dan Metabase.        |
| **Transform**      | **dbt**            | Transformasi data menggunakan SQL dan Python.               |

---

## Data Pipeline Architecture

A **data pipeline** is a series of data processing steps that move data from source systems to a destination, such as a data warehouse or data lake. The pipeline typically includes stages for data ingestion, transformation, and loading (ETL). In this project, we have implemented a data pipeline that extracts air quality and weather forecast data from a web source, processes it, and stores it in a structured format for analysis and visualization.

## Data Ingestion (Bronze Layer)

Proses _data ingestion_ pada tahap Bronze dilakukan dengan mengekstraksi data dari dua objek utama melalui metode _scraping_. Pipeline ini berjalan secara otomatis dengan interval **10 menit** menggunakan DAG: [`load_bronze`](./airflow/dags/01_bronze_load_aqi_weather.py).

---

### 1. Data Air Quality Index (Real-time)

Objek ini menyimpan informasi kualitas udara dan kondisi cuaca aktual pada saat pengambilan data.

| Kategori      | Kolom                                      | Deskripsi                                    |
| :------------ | :----------------------------------------- | :------------------------------------------- |
| **Geografis** | `province`, `city`                         | Lokasi administrasi (Provinsi dan Kota).     |
| **Polusi**    | `aqi`, `aqi_status`                        | Nilai indeks kualitas udara dan kategorinya. |
|               | `main_pollutant`, `concentration`          | Polutan dominan dan nilai konsentrasinya.    |
| **Cuaca**     | `weather`, `temperature`                   | Kondisi cuaca umum dan suhu udara.           |
|               | `humidity`, `wind_speed`, `wind_direction` | Kelembapan, kecepatan, dan arah angin.       |
| **Metadata**  | `alert`                                    | Peringatan dini terkait kualitas udara.      |
|               | `observation_time`                         | Waktu observasi asli dari sumber data.       |
|               | `scraped_at`                               | Timestamp saat data berhasil di-ingest.      |

---

### 2. Data Weather Forecast (Prediksi)

Objek ini menyimpan data prakiraan (forecast) cuaca dan kualitas udara untuk beberapa periode ke depan.

| Kategori      | Kolom                     | Deskripsi                                 |
| :------------ | :------------------------ | :---------------------------------------- |
| **Geografis** | `province`, `city`        | Lokasi administrasi (Provinsi dan Kota).  |
| **Prediksi**  | `forecast_ts`             | Target waktu/jam yang diprediksi.         |
|               | `aqi`, `weather`          | Prediksi nilai AQI dan kondisi cuaca.     |
| **Parameter** | `temperature`, `humidity` | Prediksi suhu dan kelembapan udara.       |
|               | `wind_speed`              | Prediksi kecepatan angin.                 |
| **Metadata**  | `observation_time`        | Waktu rilis data prakiraan oleh provider. |
|               | `scraped_at`              | Timestamp saat data berhasil di-ingest.   |

> ### 💡 Note
>
> Data pada Bronze Layer disimpan dalam format _raw_ (asli) untuk menjaga integritas data sebelum dilakukan transformasi lebih lanjut pada **Silver Layer**.
>
> Script _scraping_ dijalankan setiap 10 menit untuk memastikan data yang diambil selalu up-to-date, dengan catatan bahwa data prakiraan (forecast) biasanya diperbarui setiap beberapa jam sekali oleh penyedia data.
>
> Airflow DAG: [`load_bronze`](./airflow/dags/01_bronze_load_aqi_weather.py)

---

## Data Transformation (Silver Layer)

Tahap Silver Layer bertujuan untuk membersihkan, memvalidasi, dan menstandarisasi data dari Bronze Layer. Proses ini memastikan bahwa data yang akan masuk ke tahap pemodelan (Gold) memiliki kualitas tinggi dan konsisten. Pada silver layer menggunakan dbt dengan strategi _merge_ pada tabel Apache Iceberg untuk memastikan tidak ada duplikasi data.

---

### 1. De-duplication (Penghapusan Duplikasi)

Mengingat data diambil setiap 10 menit melalui proses _scraping_, terdapat potensi tumpang tindih (_overlap_) data. Maka dilakukan proses load ke silver dengan strategi _upsert_ (update + insert) untuk memastikan tidak ada duplikasi data: menggukanan dbt dengan strategi _merge_ pada tabel Apache Iceberg.

---

- **Logika:** Melakukan _drop duplicates_ berdasarkan kombinasi kunci unik:
  - **AQI:** `city` + `observation_time`.
  - **Forecast:** `city` + `forecast_ts` + `observation_time`.
- **Tujuan:** Memastikan setiap kejadian (event) hanya tercatat satu kali dalam tabel Silver.

### 2. Data Cleansing (Pembersihan Data)

Tahap ini menangani ketidakkonsistenan data mentah:

- **Handling Nulls:** Mengisi nilai yang hilang (_missing values_) atau menghapus baris yang tidak memiliki informasi krusial (seperti `aqi` atau `city`).
- **String Standardization:** Merapikan format teks pada kolom `city` dan `province` (misal: mengubah ke _lowercase_ atau menghapus spasi berlebih).
- **Filtering:** Memastikan nilai numerik seperti `aqi`, `temperature`, dan `humidity` berada dalam rentang yang masuk akal (logis).

### 3. Data Type Casting (Penyesuaian Tipe Data)

Mengubah tipe data kolom dari format _string_ (hasil scraping) ke tipe data yang sesuai untuk kebutuhan komputasi:

- **Timestamps:** Mengonversi `observation_time`, `forecast_ts`, dan `scraped_at` menjadi tipe `TIMESTAMP` (mengikuti zona waktu _Asia/Jakarta_).
- **Numerik:** Mengonversi kolom seperti `aqi`, `temperature`, dan `humidity` menjadi `INTEGER` atau `DOUBLE`.
- **Boolean/Categorical:** Menstandarisasi kolom `alert` atau `aqi_status`.

---

> ### 💡 Note
>
> Airflow DAG: [`load_staging`](./airflow/dags/02_dbt_load_aqi_weather.py)

---

## Data Modeling (Gold Layer)

Tahap Gold Layer memodelkan data ke dalam **Star Schema** untuk performa query analisis yang optimal. Proses ini dijalankan otomatis menggunakan **Airflow Datasets** dan **Trino SQL**.

### **Dimension Tables**

- **`dim_city`**: Master data kota dengan ID unik berbasis MD5 hash.
- **`dim_aqi`**: Tabel referensi kategori kesehatan kualitas udara (Good, Moderate, Unhealthy, dsb).

### **Fact Table**

- **`fact_aqi_weather`**: Tabel utama yang menggabungkan data aktual dan prediksi.
- **Optimasi**: Dipartisi berdasarkan `city_id` dan `day(event_ts)` menggunakan Apache Iceberg untuk mempercepat filter waktu pada dashboard.

---

> ### 💡 Note
>
> Airflow DAG: [`load_staging`](./airflow/dags/03_dbt_load_aqi_weather.py)

---

## How to Re-Populate

1. **Clone Repository:**

   ```bash
   git clone https://github.com/oillypump/skywatch.git
   cd skywatch
   ```

2. start all service

   it took a while to pull and build all image, so please be patient, depends on your internet connection and computer spec, it can take around 5-15 minutes to pull and build all image.
   this airflow assume local linux user id : 1000. if your user use another id please make it sure you use the same user id.

   ```bash
   docker compose pull && docker compose build --no-cache
   chmod -R 775 airflow/
   docker compose run airflow-cli airflow config list
   docker compose up airflow-init
   docker compose up -d
   ```

3. Login to Airflow UI,
   enable dan trigger DAG
   - [`01_bronze_load_aqi_weather`](./airflow/dags/01_bronze_load_aqi_weather.py) untuk memulai proses ingest data dari source.
   - [`02_dbt_load_aqi_weather`](./airflow/dags/02_dbt_load_aqi_weather.py) untuk memulai proses transformasi data ke silver layer.
   - [`03_dbt_load_aqi_weather`](./airflow/dags/03_dbt_load_aqi_weather.py) untuk memulai proses modeling data ke gold layer.

   ```
   url : http://localhost:8181
   user : airflowuser
   pass : airflowuser
   ```

   example Airflow UI:

   ![Airflow UI](./pics/airflow_ui.png)

4. Setelah beberapa saat, buka dashboard Metabase untuk melihat hasil visualisasi data kualitas udara dan prakiraan cuaca.

   ```
   url  : http://localhost:3000
   user : admin@email.com
   pass : Admin1234
   ```

   example dashboard Metabase:

   ![Metabase UI](./pics/metabase_ui.png)

5. Acces Trino with Dbeaver

   ```
   url  : http://localhost:8080
   user : admin
   pass :
   ```

   example connection Trino di Dbeaver:

   ![Trino_dbeaver](./pics/trino_dbeaver.png)

---

## How to remove all image, container, volume, network

```
docker compose down -v --remove-orphans --rmi all
```

---

## Appendix

### project tree

[Project tree](./project-tree.md)

### project hash 8aeaaf009dda8089d406b7f6ebed399b42a0a135

```
8aeaaf009dda8089d406b7f6ebed399b42a0a135
```
