# -*- coding: utf-8 -*-
import csv
import os
import random
from datetime import date, datetime, timedelta


def generate_and_save_srm_csv(
    out_dir="/opt/airflow/data",
    n_suppliers=500,
    n_departments=16,
    n_contracts=1500,
    n_po_lines=100_000,
    n_receipts=120_000,
    months=24,
    seed=42,
):
    random.seed(seed)
    os.makedirs(out_dir, exist_ok=True)

    forms = ["ООО", "АО", "ООО ТД", "ООО ПК", "ООО ТК"]
    bases = ["МеталлТрейд", "СнабСервис", "ПромКомплект", "ТехПоставка", "УралПодшипник",
             "ВолгаКрепёж", "СибЭлектро", "ЮгЛКМ", "ТрансШина", "ГидроМаркет",
             "ИндустрияПлюс", "СеверСнаб", "СтальМаркет", "ТехноРесурс", "ГрузСервис"]
    regions = ["Москва", "Московская область", "Санкт‑Петербург", "Ленинградская область",
               "Ростовская область", "Свердловская область", "Татарстан", "Нижегородская область",
               "Челябинская область", "Самарская область", "Краснодарский край", "Новосибирская область"]
    cities = ["Москва", "Химки", "Санкт‑Петербург", "Ростов‑на‑Дону", "Екатеринбург",
              "Казань", "Самара", "Новосибирск", "Краснодар", "Челябинск", "Нижний Новгород", "Гатчина"]
    pay_terms = ["Предоплата 50%", "Предоплата 100%", "Отсрочка 15 дней", "Отсрочка 30 дней", "Отсрочка 45 дней"]
    currencies = ["RUB", "RUB", "RUB", "USD", "CNY"]

    sites = ["Завод №1 (Ростов‑на‑Дону)", "Завод №2 (Ростов‑на‑Дону)", "Склад МТО (Ростов‑на‑Дону)"]
    buyers = ["Иванов И.И.", "Петров П.П.", "Сидоров А.А.", "Смирнова Е.В.", "Кузнецов Д.Н.", "Васильева А.А."]

    categories = [
        "Металлопрокат", "Крепёж", "Подшипники", "Шины и диски", "ЛКМ и химия",
        "Электрооборудование", "Гидравлика", "Услуги (логистика/ремонт)"
    ]
    items = [
        ("NOM-000001", "Лист стальной 09Г2С 6 мм", "Металлопрокат", "кг", 65, 120),
        ("NOM-000002", "Труба профильная 80×80×4", "Металлопрокат", "кг", 70, 140),
        ("NOM-000003", "Болт М12×40 оцинкованный 8.8", "Крепёж", "шт", 4, 18),
        ("NOM-000004", "Гайка М12 самоконтрящаяся", "Крепёж", "шт", 3, 14),
        ("NOM-000005", "Подшипник 30210", "Подшипники", "шт", 450, 1800),
        ("NOM-000006", "Шина 385/65 R22.5", "Шины и диски", "шт", 18000, 45000),
        ("NOM-000007", "Эмаль полиуретановая (комплект)", "ЛКМ и химия", "компл", 3500, 12000),
        ("NOM-000008", "Растворитель универсальный", "ЛКМ и химия", "л", 140, 520),
        ("NOM-000009", "Кабель КГ 3×2.5", "Электрооборудование", "м", 90, 240),
        ("NOM-000010", "Фара рабочая LED 24В", "Электрооборудование", "шт", 900, 4200),
        ("NOM-000011", "Рукав РВД 2SN 1/2\" с фитингами", "Гидравлика", "шт", 650, 2600),
        ("NOM-000012", "Услуга: доставка (фура, 20 т)", "Услуги (логистика/ремонт)", "рейс", 18000, 85000),
    ]
    cat_to_dept_default = {
        "Металлопрокат": "DEP-001", "Крепёж": "DEP-002", "Подшипники": "DEP-005",
        "Шины и диски": "DEP-007", "ЛКМ и химия": "DEP-003",
        "Электрооборудование": "DEP-004", "Гидравлика": "DEP-005",
        "Услуги (логистика/ремонт)": "DEP-008",
    }

    end = date.today()
    start = end - timedelta(days=30 * months)

    def rdate(a, b):
        days = max((b - a).days, 0)
        return a + timedelta(days=random.randint(0, days))

    def digits(n):
        return "".join(str(random.randint(0, 9)) for _ in range(n))

    # ---------- departments.csv ----------
    departments = []
    predefined = [
        "Производство", "Сварочно-сборочный цех", "Покрасочный участок", "Энергетическая служба",
        "Служба главного механика", "ОТК (контроль качества)", "МТО/Складское хозяйство", "Логистика"
    ]
    while len(predefined) < n_departments:
        predefined.append(f"Подразделение {len(predefined)+1}")
    for i, name in enumerate(predefined[:n_departments], start=1):
        departments.append({
            "dept_id": f"DEP-{i:03d}",
            "dept_name": name,
            "plant": random.choice(sites),
            "cost_center": f"CC-{1000+i}",
            "manager": random.choice(["Иванов С.С.", "Петров К.К.", "Смирнова Н.Н.", "Кузнецов М.М.", "Сидоров Р.Р.", "Васильева А.А."]),
            "status": "Действует",
        })
    with open(os.path.join(out_dir, "departments.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(departments[0].keys()))
        w.writeheader()
        w.writerows(departments)

    dept_ids = [d["dept_id"] for d in departments]

    # ---------- suppliers.csv ----------
    suppliers = []
    bad_supplier_share = 0.20
    for i in range(1, n_suppliers + 1):
        form = random.choice(forms)
        base = random.choice(bases)
        suffix = random.choice(["", "", "Снаб", "Трейд", "Групп", "Плюс"])
        name = f'{form} "{base}{suffix}"'
        created = rdate(start, end - timedelta(days=60))
        updated = rdate(created, end)
        suppliers.append({
            "supplier_id": f"SUP-{i:05d}",
            "supplier_name": name if random.random() > 0.15 else name.replace('"', ''),
            "inn": digits(10),
            "kpp": digits(9),
            "ogrn": digits(13),
            "region": random.choice(regions),
            "city": random.choice(cities),
            "status": random.choices(["Действующий", "Действующий", "Заблокирован"], weights=[85, 10, 5])[0],
            "payment_terms": random.choice(pay_terms),
            "created_at": created.isoformat(),
            "updated_at": updated.isoformat(),
        })

    supplier_ids = [s["supplier_id"] for s in suppliers]

    # какие поставщики "плохие"
    bad_supplier_ids = set(random.sample(supplier_ids, int(round(n_suppliers * bad_supplier_share))))
    is_bad = {sid: (sid in bad_supplier_ids) for sid in supplier_ids}

    # целевые доли просроченных ЗАКАЗОВ:
    # плохие: ~10%, нормальные: ~1% (с небольшим шумом на каждого поставщика)
    delay_order_rate = {}
    for sid in supplier_ids:
        if is_bad[sid]:
            delay_order_rate[sid] = random.uniform(0.08, 0.12)
        else:
            delay_order_rate[sid] = random.uniform(0.005, 0.015)

    # веса (парето) — чтобы небольшая часть поставщиков имела много заказов
    alpha = 1.16
    raw_w = [random.paretovariate(alpha) for _ in supplier_ids]
    s_w = sum(raw_w)
    supplier_weights = [w / s_w for w in raw_w]

    payment_by_supplier = {s["supplier_id"]: s["payment_terms"] for s in suppliers}

    with open(os.path.join(out_dir, "suppliers.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(suppliers[0].keys()))
        w.writeheader()
        w.writerows(suppliers)

    # ---------- contracts.csv ----------
    contracts = []
    for i in range(1, n_contracts + 1):
        sid = random.choices(supplier_ids, weights=supplier_weights)[0]
        start_dt = rdate(start, end - timedelta(days=120))
        duration = random.randint(180, 540)
        end_dt = start_dt + timedelta(days=duration)

        # целево ~80–85% действующих, остальное истёкшие
        if random.random() < 0.82:
            # гарантируем, что конец в будущем
            end_dt = max(end_dt, date.today() + timedelta(days=random.randint(30, 360)))
            status = "Действует"
        else:
            # гарантируем, что конец в прошлом
            end_dt = min(end_dt, date.today() - timedelta(days=random.randint(1, 360)))
            status = "Истёк"

        contracts.append({
            "contract_id": f"CTR-{i:06d}",
            "supplier_id": sid,
            "contract_no": f"ДОГ-{random.randint(1000, 9999)}/{start_dt.year}",
            "category": random.choice(categories),
            "currency": random.choice(currencies),
            "agreed_lead_time_days": random.choice([7, 10, 14, 21, 30, 45]),
            "payment_terms": payment_by_supplier[sid],
            "start_date": start_dt.isoformat(),
            "end_date": min(end_dt, end + timedelta(days=365)).isoformat(),
            "status": status,
        })

    with open(os.path.join(out_dir, "contracts.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(contracts[0].keys()))
        w.writeheader()
        w.writerows(contracts)

    contracts_by_supplier = {}
    for c in contracts:
        contracts_by_supplier.setdefault(c["supplier_id"], []).append(c)

    # ---------- po_lines.csv ----------
    po_lines_path = os.path.join(out_dir, "po_lines.csv")
    po_line_rows_meta = []

    with open(po_lines_path, "w", newline="", encoding="utf-8") as f:
        fields = ["po_id", "po_line_id", "po_date", "supplier_id", "contract_id", "dept_id",
                  "site", "buyer", "item_id", "item_name", "category", "uom", "qty_ordered",
                  "unit_price", "amount", "requested_delivery_date", "promised_delivery_date", "po_status"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()

        po_seq = 1
        line_seq = 1

        line_counts = [1, 2, 3, 4, 5]
        line_weights = [0.55, 0.22, 0.12, 0.07, 0.04]

        while line_seq <= n_po_lines:
            po_id = f"PO-{po_seq:07d}"
            po_seq += 1

            sid = random.choices(supplier_ids, weights=supplier_weights)[0]
            cands = contracts_by_supplier.get(sid, [])

            active_contracts = [c for c in cands if c.get("status") == "Действует"]
            expired_contracts = [c for c in cands if c.get("status") != "Действует"]

            base_no_ctr = random.uniform(0.04, 0.06)
            base_expired = random.uniform(0.10, 0.14)

            if is_bad[sid]:
                base_no_ctr = min(base_no_ctr * 1.5, 0.10)
                base_expired = min(base_expired * 1.3, 0.25)

            p_no_ctr = base_no_ctr
            p_expired = base_expired

            r = random.random()
            if not cands:
                contract_id_for_order = ""
            else:
                if r < p_no_ctr:
                    contract_id_for_order = ""
                else:
                    use_expired = (random.random() < p_expired) and len(expired_contracts) > 0
                    if use_expired:
                        contract_id_for_order = random.choice(expired_contracts)["contract_id"]
                    else:
                        src = active_contracts or cands
                        contract_id_for_order = random.choice(src)["contract_id"]

            po_date = rdate(start, end)
            site = random.choice(sites)
            buyer = random.choice(buyers)

            order_is_delayed = (random.random() < delay_order_rate[sid])

            n_lines = random.choices(line_counts, weights=line_weights)[0]
            n_lines = min(n_lines, n_po_lines - line_seq + 1)

            delayed_lines_in_order = set()
            if order_is_delayed:
                k = 1 if n_lines == 1 else random.choice([1, 1, 2])
                delayed_lines_in_order = set(random.sample(range(n_lines), k))

            for i in range(n_lines):
                it = random.choice(items)
                item_id, item_name, cat, uom, pmin, pmax = it
                dept_id = cat_to_dept_default.get(cat, random.choice(dept_ids))

                if uom in ["кг", "м"]:
                    qty = int(random.lognormvariate(5, 1.2))
                    qty = max(10, min(qty, 10000))
                else:
                    qty = int(random.lognormvariate(2.5, 1.0))
                    qty = max(1, min(qty, 500))

                price = round(random.uniform(pmin, pmax), 2)
                amount = round(qty * price, 2)

                requested = po_date + timedelta(days=random.randint(7, 35))
                lead = random.choice([10, 14, 21, 30, 45])
                if contract_id_for_order:
                    c = next((x for x in cands if x["contract_id"] == contract_id_for_order), None)
                    if c:
                        try:
                            lead = int(c["agreed_lead_time_days"])
                        except Exception:
                            pass
                promised = po_date + timedelta(days=lead + random.randint(-2, 10))

                po_status = random.choices(["Открыт", "Закрыт", "Отменён"], weights=[20, 75, 5])[0]

                po_line_id = f"POL-{line_seq:09d}"
                row = {
                    "po_id": po_id,
                    "po_line_id": po_line_id,
                    "po_date": po_date.isoformat(),
                    "supplier_id": sid,
                    "contract_id": contract_id_for_order,
                    "dept_id": dept_id,
                    "site": site,
                    "buyer": buyer,
                    "item_id": item_id,
                    "item_name": item_name,
                    "category": cat,
                    "uom": uom,
                    "qty_ordered": qty,
                    "unit_price": price,
                    "amount": amount,
                    "requested_delivery_date": requested.isoformat(),
                    "promised_delivery_date": promised.isoformat(),
                    "po_status": po_status,
                }
                w.writerow(row)

                if po_status != "Отменён":
                    po_line_rows_meta.append({
                        "po_id": po_id,
                        "po_line_id": po_line_id,
                        "supplier_id": sid,
                        "po_date": po_date,
                        "promised_date": promised,
                        "qty_ordered": qty,
                        "is_delayed_line": (i in delayed_lines_in_order),
                    })

                line_seq += 1

    # ---------- receipts_qc.csv ----------
    base = len(po_line_rows_meta)
    if n_receipts < base:
        raise ValueError(f"n_receipts={n_receipts} меньше числа строк заказа={base}")

    extra = n_receipts - base
    extra_idx = set(random.sample(range(base), extra)) if extra > 0 else set()

    receipts_path = os.path.join(out_dir, "receipts_qc.csv")
    with open(receipts_path, "w", newline="", encoding="utf-8") as f:
        fields = ["receipt_id", "po_id", "po_line_id", "receipt_date", "warehouse",
                  "qty_received", "qty_accepted", "qty_rejected", "reject_reason"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()

        warehouses = ["Склад МТО", "Склад металла", "Склад ЛКМ", "Склад комплектующих"]
        reasons = ["Вмятины/дефект поверхности", "Несоответствие размеру", "Нарушение упаковки", "Брак партии"]

        rid = 1
        for idx, m in enumerate(po_line_rows_meta):
            sid = m["supplier_id"]
            promised = m["promised_date"]
            po_date = m["po_date"]
            qty_total = m["qty_ordered"]
            delayed = m["is_delayed_line"]

            if delayed:
                delay_days = random.randint(5, 25) if is_bad[sid] else random.randint(1, 7)
                last_receipt_date = promised + timedelta(days=delay_days)
            else:
                early = random.randint(0, 3)
                last_receipt_date = promised - timedelta(days=early)

            min_date = po_date + timedelta(days=1)
            if last_receipt_date < min_date:
                last_receipt_date = min_date

            n_parts = 2 if idx in extra_idx else 1

            if n_parts == 1:
                parts = [qty_total]
                dates = [last_receipt_date]
            else:
                first = max(1, int(qty_total * random.uniform(0.3, 0.7)))
                second = qty_total - first
                parts = [first, second]
                d1 = last_receipt_date - timedelta(days=random.randint(1, 7))
                if d1 < min_date:
                    d1 = min_date
                if (not delayed) and d1 > promised:
                    d1 = promised
                dates = [d1, last_receipt_date]

            for part_qty, rdate_ in zip(parts, dates):
                if is_bad[sid]:
                    defect_rate = random.choices([random.uniform(0.0, 0.02),
                                                  random.uniform(0.02, 0.07),
                                                  random.uniform(0.07, 0.15)], weights=[55, 35, 10])[0]
                else:
                    defect_rate = random.choices([random.uniform(0.0, 0.01),
                                                  random.uniform(0.01, 0.03),
                                                  random.uniform(0.03, 0.06)], weights=[85, 13, 2])[0]

                rejected = min(int(round(part_qty * defect_rate)), part_qty)
                accepted = part_qty - rejected

                w.writerow({
                    "receipt_id": f"RCPT-{rid:09d}",
                    "po_id": m["po_id"],
                    "po_line_id": m["po_line_id"],
                    "receipt_date": rdate_.isoformat(),
                    "warehouse": random.choice(warehouses),
                    "qty_received": part_qty,
                    "qty_accepted": accepted,
                    "qty_rejected": rejected,
                    "reject_reason": "" if rejected == 0 else random.choice(reasons),
                })
                rid += 1

    print("SRM CSV generated:", out_dir, sep="\n  ")
