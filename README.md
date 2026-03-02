# 📊 Atlas Coop ETL Pipeline: Excel to PostgreSQL

An automated, multi-stage Data Engineering ETL (Extract, Transform, Load) pipeline built in Python. This project extracts messy, real-world transactional data from Excel, applies rigorous data cleaning and validation, and securely loads it into a relational PostgreSQL database using a robust staging architecture.

## 🚀 Project Overview
The goal of this project is to process cooperative company data (Customers, Products, and Orders) from raw `.xlsx` files into a production-ready SQL database. The pipeline is designed to be fully idempotent, highly resilient to "dirty data," and generates automated diagnostic reports for any rejected records.

## 🏗️ Architecture & How It Works

This pipeline follows a strict, multi-stage Data Engineering pattern:

1. **Extract**: Reads raw data from multi-sheet Excel files using `pandas` and `openpyxl`.
2. **Staging Load**: Inserts raw, unvalidated data directly into PostgreSQL staging tables (`atlas.stg_orders`, etc.) for auditing.
3. **Transform & Validate**:
   - Standardizes messy datetime formats.
   - Normalizes phone numbers and strips hidden whitespace to protect SQL `JOIN` integrity.
   - Coerces missing/blank numeric cells into safe SQL `NULL` values.
   - Enforces referential integrity (e.g., verifying `customer_code` exists before processing an order).
4. **Production Load**: Uses `SQLAlchemy` and `psycopg3` to perform bulk `UPSERT` operations (`ON CONFLICT DO NOTHING`) into the final production schema.
5. **Rejects Logging**: Automatically serializes any failed or invalid rows into a JSONB `atlas.rejected_rows` table for easy debugging, while allowing the rest of the batch to succeed.

## 🧠 Key Data Engineering Challenges Solved

* **The "Float NaN" DBAPI Trap:** Solved a known `psycopg3` strict type-checking crash by forcing Pandas to cast empty numerical Excel cells (`NaN`) into standard Python `None` objects before SQL insertion.
* **Invisible Whitespace & Header Typos:** Engineered automated cleaning functions to strip hidden characters and standardize column headers mid-flight, preventing silent Foreign Key validation failures.
* **JSON Timestamp Serialization:** Overcame Python `json.dumps()` limitations by writing a custom encoder to serialize Pandas Timestamp objects for the rejected-rows logging system.
* **Parameter Overflow:** Optimized the SQL bulk-insert strategy to bypass PostgreSQL's 65,535 parameter limit for high-volume order data.

## 💻 Tech Stack
* **Language:** Python 3.12
* **Libraries:** `pandas`, `SQLAlchemy`, `psycopg` (v3), `python-dotenv`, `openpyxl`
* **Database:** PostgreSQL 16+
* **Environment:** Virtual Environments (`.venv`)

## 📂 Database Schema (Atlas)
* `customers`: Stores validated customer details.
* `products`: Stores active inventory and pricing.
* `orders`: Core transaction headers.
* `order_items`: Line items linking orders to products with historical unit prices.
* `payments`: Transaction payment methods and currencies.
* `rejected_rows`: A quarantine table using `JSONB` to store invalid records and their specific error reasons.

## 📸 Proof of Concept & Screenshots

*(Add your screenshots below! Replace the placeholder links with your actual image paths once uploaded to your repo)*

### 1. The Terminal Execution & Automated Reporting
> Shows the script successfully extracting 10,000+ rows and generating the diagnostic Markdown report.
![ETL Terminal Execution](path/to/terminal_screenshot.png)

### 2. The Final PostgreSQL Database
> Showing the populated production tables and relationships.
![PostgreSQL Database Proof](path/to/database_screenshot.png)

### 3. The Rejects Log (Handling Bad Data)
> Showing how the system handles missing data without crashing.
![JSONB Rejects Table](path/to/rejects_screenshot.png)

## ⚙️ How to Run Locally

1. Clone this repository:
   ```bash
   git clone [https://github.com/ZackDev10/atlas-excel-to-postgres.git](https://github.com/)
