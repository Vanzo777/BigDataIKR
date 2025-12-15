-- srm_build_dm.sql
-- Строит DM-таблицы, которые напрямую отвечают на три бизнес-вопроса





CREATE SCHEMA IF NOT EXISTS dm;

----------------------------------------------------------------------
-- Q1. Какие поставщики чаще всего срывают сроки поставки и на сколько дней?
-- dm.dm_q1_supplier_delay
----------------------------------------------------------------------

DROP TABLE IF EXISTS dm.dm_q1_supplier_delay;
CREATE TABLE dm.dm_q1_supplier_delay AS
WITH line_last_receipt AS (
    SELECT
        r.po_line_id,
        MAX(r.receipt_date) AS last_receipt_date
    FROM dds.receipts_qc r
    GROUP BY r.po_line_id
),
line_delay AS (
    SELECT
        p.supplier_id,
        s.supplier_name,
        GREATEST(
            (lr.last_receipt_date::date - p.promised_delivery_date::date),
            0
        )::integer AS delay_days
    FROM dds.po_lines p
    JOIN line_last_receipt lr ON lr.po_line_id = p.po_line_id
    LEFT JOIN dds.suppliers s ON s.supplier_id = p.supplier_id
)
SELECT
    supplier_id,
    supplier_name,
    COUNT(*) AS lines_total,
    SUM(CASE WHEN delay_days > 0 THEN 1 ELSE 0 END) AS lines_delayed,
    ROUND(
        100.0 * SUM(CASE WHEN delay_days > 0 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    ) AS delayed_share_pct,
    ROUND(AVG(delay_days)::numeric, 2) AS avg_delay_days,
    MAX(delay_days) AS max_delay_days
FROM line_delay
GROUP BY supplier_id, supplier_name
ORDER BY delayed_share_pct DESC, avg_delay_days DESC;

----------------------------------------------------------------------
-- Q2. У каких поставщиков самая высокая доля брака и какие типовые причины?
-- 2.1 Сводка по доле брака по поставщикам: dm.dm_q2_supplier_rejects
-- 2.2 Разбивка по причинам брака:      dm.dm_q2_supplier_reject_reasons
----------------------------------------------------------------------

DROP TABLE IF EXISTS dm.dm_q2_supplier_rejects;
CREATE TABLE dm.dm_q2_supplier_rejects AS
SELECT
    p.supplier_id,
    s.supplier_name,
    SUM(r.qty_received) AS qty_received_total,
    SUM(r.qty_rejected) AS qty_rejected_total,
    ROUND(
        100.0 * SUM(r.qty_rejected)
        / NULLIF(SUM(r.qty_received), 0),
        2
    ) AS reject_share_pct
FROM dds.receipts_qc r
JOIN dds.po_lines  p ON p.po_line_id = r.po_line_id
LEFT JOIN dds.suppliers s ON s.supplier_id = p.supplier_id
GROUP BY p.supplier_id, s.supplier_name
ORDER BY reject_share_pct DESC, qty_received_total DESC;

DROP TABLE IF EXISTS dm.dm_q2_supplier_reject_reasons;
CREATE TABLE dm.dm_q2_supplier_reject_reasons AS
SELECT
    p.supplier_id,
    s.supplier_name,
    COALESCE(r.reject_reason, 'НЕ УКАЗАНО') AS reject_reason,
    SUM(r.qty_rejected) AS qty_rejected_total
FROM dds.receipts_qc r
JOIN dds.po_lines  p ON p.po_line_id = r.po_line_id
LEFT JOIN dds.suppliers s ON s.supplier_id = p.supplier_id
GROUP BY
    p.supplier_id,
    s.supplier_name,
    COALESCE(r.reject_reason, 'НЕ УКАЗАНО')
ORDER BY qty_rejected_total DESC;

----------------------------------------------------------------------
-- Q3. Доля закупок по видам договоров и кто чаще «вне договора»
-- 3.1 Общая доля по видам договоров: dm.dm_q3_contract_share
-- 3.2 Поставщики с высокой долей «вне договора»: dm.dm_q3_no_contract_suppliers
-- 3.3 Подразделения с высокой долей «вне договора»: dm.dm_q3_no_contract_depts
----------------------------------------------------------------------

DROP TABLE IF EXISTS dm.dm_q3_contract_share;
CREATE TABLE dm.dm_q3_contract_share AS
SELECT
    contract_flag,                         -- in_contract / expired / no_contract
    SUM(amount) AS amount_total,
    ROUND(
        100.0 * SUM(amount)
        / NULLIF(SUM(SUM(amount)) OVER (), 0),
        2
    ) AS share_pct
FROM dds.po_lines
GROUP BY contract_flag
ORDER BY contract_flag;

DROP TABLE IF EXISTS dm.dm_q3_no_contract_suppliers;
CREATE TABLE dm.dm_q3_no_contract_suppliers AS
SELECT
    p.supplier_id,
    s.supplier_name,
    SUM(CASE WHEN p.contract_flag = 'no_contract' THEN p.amount ELSE 0 END) AS amount_no_contract,
    SUM(p.amount) AS amount_total,
    ROUND(
        100.0 * SUM(CASE WHEN p.contract_flag = 'no_contract' THEN p.amount ELSE 0 END)
        / NULLIF(SUM(p.amount), 0),
        2
    ) AS no_contract_share_pct
FROM dds.po_lines p
LEFT JOIN dds.suppliers s ON s.supplier_id = p.supplier_id
GROUP BY p.supplier_id, s.supplier_name
HAVING SUM(CASE WHEN p.contract_flag = 'no_contract' THEN p.amount ELSE 0 END) > 0
ORDER BY no_contract_share_pct DESC, amount_no_contract DESC;

DROP TABLE IF EXISTS dm.dm_q3_no_contract_depts;
CREATE TABLE dm.dm_q3_no_contract_depts AS
SELECT
    p.dept_id,
    d.dept_name,
    SUM(CASE WHEN p.contract_flag = 'no_contract' THEN p.amount ELSE 0 END) AS amount_no_contract,
    SUM(p.amount) AS amount_total,
    ROUND(
        100.0 * SUM(CASE WHEN p.contract_flag = 'no_contract' THEN p.amount ELSE 0 END)
        / NULLIF(SUM(p.amount), 0),
        2
    ) AS no_contract_share_pct
FROM dds.po_lines p
LEFT JOIN dds.departments d ON d.dept_id = p.dept_id
GROUP BY p.dept_id, d.dept_name
HAVING SUM(CASE WHEN p.contract_flag = 'no_contract' THEN p.amount ELSE 0 END) > 0
ORDER BY no_contract_share_pct DESC, amount_no_contract DESC;
