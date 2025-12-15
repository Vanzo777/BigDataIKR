# -*- coding: utf-8 -*-
import psycopg2
import os


def transform_stg_to_dds():
    """
    Создаёт схему dds, применяет трансформации:
    - Типизация (text → date/numeric/integer)
    - TRIM + NULLIF для текста
    - Добавление вычисляемых полей (contract_flag, is_rejected)
    - Дедупликация по бизнес-ключам
    - Добавление load_dttm
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Создать схему dds
    cur.execute("CREATE SCHEMA IF NOT EXISTS dds;")

    # ---------- dds.suppliers ----------
    cur.execute("""
    DROP TABLE IF EXISTS dds.suppliers CASCADE;
    CREATE TABLE dds.suppliers AS
    WITH dedup AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY updated_at DESC) AS rn
      FROM stg.suppliers
    )
    SELECT
      supplier_id,
      NULLIF(TRIM(supplier_name), '') AS supplier_name,
      NULLIF(TRIM(inn), '') AS inn,
      NULLIF(TRIM(kpp), '') AS kpp,
      NULLIF(TRIM(ogrn), '') AS ogrn,
      NULLIF(TRIM(region), '') AS region,
      NULLIF(TRIM(city), '') AS city,
      NULLIF(TRIM(status), '') AS status,
      NULLIF(TRIM(payment_terms), '') AS payment_terms,
      created_at::date AS created_at,
      updated_at::date AS updated_at,
      CURRENT_TIMESTAMP AS load_dttm
    FROM dedup
    WHERE rn = 1;
    """)

    # ---------- dds.departments ----------
    cur.execute("""
    DROP TABLE IF EXISTS dds.departments CASCADE;
    CREATE TABLE dds.departments AS
    WITH dedup AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY dept_id) AS rn
      FROM stg.departments
    )
    SELECT
      dept_id,
      NULLIF(TRIM(dept_name), '') AS dept_name,
      NULLIF(TRIM(plant), '') AS plant,
      NULLIF(TRIM(cost_center), '') AS cost_center,
      NULLIF(TRIM(manager), '') AS manager,
      NULLIF(TRIM(status), '') AS status,
      CURRENT_TIMESTAMP AS load_dttm
    FROM dedup
    WHERE rn = 1;
    """)

    # ---------- dds.contracts ----------
    cur.execute("""
    DROP TABLE IF EXISTS dds.contracts CASCADE;
    CREATE TABLE dds.contracts AS
    WITH dedup AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY contract_id) AS rn
      FROM stg.contracts
    )
    SELECT
      contract_id,
      supplier_id,
      NULLIF(TRIM(contract_no), '') AS contract_no,
      NULLIF(TRIM(category), '') AS category,
      NULLIF(TRIM(currency), '') AS currency,
      COALESCE(agreed_lead_time_days::integer, 0) AS agreed_lead_time_days,
      NULLIF(TRIM(payment_terms), '') AS payment_terms,
      start_date::date AS start_date,
      end_date::date AS end_date,
      NULLIF(TRIM(status), '') AS status,
      CURRENT_TIMESTAMP AS load_dttm
    FROM dedup
    WHERE rn = 1;
    """)

    # ---------- dds.po_lines ----------
    cur.execute("""
    DROP TABLE IF EXISTS dds.po_lines CASCADE;
    CREATE TABLE dds.po_lines AS
    WITH dedup AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY po_line_id ORDER BY po_line_id) AS rn
      FROM stg.po_lines
    )
    SELECT
      po_id,
      po_line_id,
      po_date::date AS po_date,
      supplier_id,
      NULLIF(TRIM(contract_id), '') AS contract_id,
      dept_id,
      NULLIF(TRIM(site), '') AS site,
      NULLIF(TRIM(buyer), '') AS buyer,
      NULLIF(TRIM(item_id), '') AS item_id,
      NULLIF(TRIM(item_name), '') AS item_name,
      NULLIF(TRIM(category), '') AS category,
      NULLIF(TRIM(uom), '') AS uom,
      qty_ordered::integer AS qty_ordered,
      unit_price::numeric AS unit_price,
      amount::numeric AS amount,
      requested_delivery_date::date AS requested_delivery_date,
      promised_delivery_date::date AS promised_delivery_date,
      NULLIF(TRIM(po_status), '') AS po_status,
      
      -- Добавляем вычисляемое поле contract_flag
      CASE
        WHEN NULLIF(TRIM(contract_id), '') IS NULL THEN 'no_contract'
        WHEN EXISTS (
          SELECT 1 FROM stg.contracts c
          WHERE c.contract_id = NULLIF(TRIM(stg.po_lines.contract_id), '')
            AND c.status = 'Действует'
        ) THEN 'in_contract'
        ELSE 'expired'
      END AS contract_flag,
      
      CURRENT_TIMESTAMP AS load_dttm
    FROM dedup
    WHERE rn = 1;
    """)

    # ---------- dds.receipts_qc ----------
    cur.execute("""
    DROP TABLE IF EXISTS dds.receipts_qc CASCADE;
    CREATE TABLE dds.receipts_qc AS
    WITH dedup AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY receipt_id ORDER BY receipt_id) AS rn
      FROM stg.receipts_qc
    )
    SELECT
      receipt_id,
      po_id,
      po_line_id,
      receipt_date::date AS receipt_date,
      NULLIF(TRIM(warehouse), '') AS warehouse,
      qty_received::integer AS qty_received,
      qty_accepted::integer AS qty_accepted,
      qty_rejected::integer AS qty_rejected,
      NULLIF(TRIM(reject_reason), '') AS reject_reason,
      
      -- Добавляем флаг брака
      CASE WHEN qty_rejected > 0 THEN true ELSE false END AS is_rejected,
      
      CURRENT_TIMESTAMP AS load_dttm
    FROM dedup
    WHERE rn = 1;
    """)

    cur.close()
    conn.close()
    print("DDS schema created and data transformed successfully.")
