CREATE SCHEMA IF NOT EXISTS dds;

-- suppliers
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

-- departments
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

-- contracts
DROP TABLE IF EXISTS dds.contracts CASCADE;
CREATE TABLE dds.contracts AS
WITH dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY contract_id) AS rn
    FROM stg.contracts
)
SELECT
    d.contract_id,
    d.supplier_id,
    NULLIF(TRIM(d.contract_no), '') AS contract_no,
    NULLIF(TRIM(d.category), '') AS category,
    NULLIF(TRIM(d.currency), '') AS currency,
    d.agreed_lead_time_days::integer AS agreed_lead_time_days,
    NULLIF(TRIM(d.payment_terms), '') AS payment_terms,
    d.start_date::date AS start_date,
    d.end_date::date AS end_date,
    NULLIF(TRIM(d.status), '') AS status,
    CURRENT_TIMESTAMP AS load_dttm
FROM dedup d
WHERE d.rn = 1;


-- po_lines
DROP TABLE IF EXISTS dds.po_lines CASCADE;
CREATE TABLE dds.po_lines AS
WITH dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY po_line_id ORDER BY po_line_id) AS rn
    FROM stg.po_lines
)
SELECT
    p.po_id,
    p.po_line_id,
    p.po_date::date AS po_date,
    p.supplier_id,
    NULLIF(TRIM(p.contract_id), '') AS contract_id,
    p.dept_id,
    NULLIF(TRIM(p.site), '') AS site,
    NULLIF(TRIM(p.buyer), '') AS buyer,
    NULLIF(TRIM(p.item_id), '') AS item_id,
    NULLIF(TRIM(p.item_name), '') AS item_name,
    NULLIF(TRIM(p.category), '') AS category,
    NULLIF(TRIM(p.uom), '') AS uom,
    p.qty_ordered::integer AS qty_ordered,
    p.unit_price::numeric AS unit_price,
    p.amount::numeric AS amount,
    p.requested_delivery_date::date AS requested_delivery_date,
    p.promised_delivery_date::date AS promised_delivery_date,
    NULLIF(TRIM(p.po_status), '') AS po_status,
    CASE
        WHEN NULLIF(TRIM(p.contract_id), '') IS NULL THEN 'no_contract'
        WHEN c.status = 'Действует' THEN 'in_contract'
        ELSE 'expired'
    END AS contract_flag,
    CURRENT_TIMESTAMP AS load_dttm
FROM dedup p
LEFT JOIN stg.contracts c
  ON c.contract_id = NULLIF(TRIM(p.contract_id), '')
WHERE p.rn = 1;


-- receipts_qc
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
    (qty_rejected::integer > 0) AS is_rejected,
    CURRENT_TIMESTAMP AS load_dttm
FROM dedup
WHERE rn = 1;

