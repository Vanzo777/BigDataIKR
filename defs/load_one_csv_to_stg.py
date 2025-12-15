# -*- coding: utf-8 -*-
import os
from typing import Dict
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Описание stg-таблиц и порядок загрузки
STG_SPECS: Dict[str, Dict] = {
    "suppliers": {
        "file": "suppliers.csv",
        "ddl": """
        CREATE TABLE IF NOT EXISTS stg.suppliers (
            supplier_id   TEXT,
            supplier_name TEXT,
            inn           TEXT,
            kpp           TEXT,
            ogrn          TEXT,
            region        TEXT,
            city          TEXT,
            status        TEXT,
            payment_terms TEXT,
            created_at    TEXT,
            updated_at    TEXT
        );
        """,
    },
    "contracts": {
        "file": "contracts.csv",
        "ddl": """
        CREATE TABLE IF NOT EXISTS stg.contracts (
            contract_id            TEXT,
            supplier_id            TEXT,
            contract_no            TEXT,
            category               TEXT,
            currency               TEXT,
            agreed_lead_time_days  TEXT,
            payment_terms          TEXT,
            start_date             TEXT,
            end_date               TEXT,
            status                 TEXT
        );
        """,
    },
    "departments": {
        "file": "departments.csv",
        "ddl": """
        CREATE TABLE IF NOT EXISTS stg.departments (
            dept_id     TEXT,
            dept_name   TEXT,
            plant       TEXT,
            cost_center TEXT,
            manager     TEXT,
            status      TEXT
        );
        """,
    },
    "po_lines": {
        "file": "po_lines.csv",
        "ddl": """
        CREATE TABLE IF NOT EXISTS stg.po_lines (
            po_id                   TEXT,
            po_line_id              TEXT,
            po_date                 TEXT,
            supplier_id             TEXT,
            contract_id             TEXT,
            dept_id                 TEXT,
            site                    TEXT,
            buyer                   TEXT,
            item_id                 TEXT,
            item_name               TEXT,
            category                TEXT,
            uom                     TEXT,
            qty_ordered             TEXT,
            unit_price              TEXT,
            amount                  TEXT,
            requested_delivery_date TEXT,
            promised_delivery_date  TEXT,
            po_status               TEXT
        );
        """,
    },
    "receipts_qc": {
        "file": "receipts_qc.csv",
        "ddl": """
        CREATE TABLE IF NOT EXISTS stg.receipts_qc (
            receipt_id    TEXT,
            po_id         TEXT,
            po_line_id    TEXT,
            receipt_date  TEXT,
            warehouse     TEXT,
            qty_received  TEXT,
            qty_accepted  TEXT,
            qty_rejected  TEXT,
            reject_reason TEXT
        );
        """,
    },
}


def _copy_csv(conn, table_fq: str, file_path: str, delimiter: str, encoding: str):
    # COPY FROM STDIN быстрый и надёжный для больших CSV; используем psycopg2.copy_expert через Hook
    # и обязательно сами проверяем наличие файла (copy_expert не падает, если файла нет) [web:219][web:277].
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    with open(file_path, "r", encoding=encoding) as f, conn.cursor() as cur:
        sql = (
            f"COPY {table_fq} FROM STDIN WITH CSV HEADER "
            f"DELIMITER '{delimiter}' ENCODING 'UTF8';"
        )
        cur.copy_expert(sql=sql, file=f)


def load_all_csv_to_stg(
    postgres_conn_id: str,
    data_dir: str = "/opt/airflow/data",
    delimiter: str = ",",
    encoding: str = "utf-8",
):
    """
    Создаёт схему stg, при необходимости создаёт таблицы из STG_SPECS,
    делает TRUNCATE и загружает все найденные CSV из data_dir.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()

    # 1) Схема stg
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS stg;")
    conn.commit()

    # 2) Таблицы + TRUNCATE + COPY по наличию файлов
    for table_key, spec in STG_SPECS.items():
        table_fq = f"stg.{table_key}"
        file_path = os.path.join(data_dir, spec["file"])

        # создать таблицу, если нет
        with conn.cursor() as cur:
            cur.execute(spec["ddl"])
        conn.commit()

        # если файла нет — просто пропускаем конкретную таблицу
        if not os.path.exists(file_path):
            # можно логировать предупреждение, но не падать
            print(f"[WARN] Skip {table_fq}: file not found {file_path}")
            continue

        # очистить и загрузить
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_fq};")
        conn.commit()

        _copy_csv(conn, table_fq, file_path, delimiter, encoding)
        conn.commit()

    # завершение
    try:
        conn.close()
    except Exception:
        pass
