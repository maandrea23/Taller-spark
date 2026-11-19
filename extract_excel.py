"""Convierte las hojas del Excel del taller a CSV y JSONL para Spark."""
from __future__ import annotations

import csv
import json
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
SHEETS = {
    "clientes": "customers.csv",
    "productos": "products.csv",
    "pedidos": "orders.csv",
    "detalle_pedidos": "order_items.csv",
    "pedidos_incrementales": "orders_incremental.jsonl",
    "detalle_incremental": "order_items_incremental.jsonl",
}


def cell_value(cell: ET.Element) -> str:
    inline = cell.find("m:is", NS)
    if inline is not None:
        return "".join(t.text or "" for t in inline.findall(".//m:t", NS))
    value = cell.find("m:v", NS)
    return "" if value is None else (value.text or "")


def read_sheet(zf: zipfile.ZipFile, sheet_no: int) -> list[list[str]]:
    root = ET.fromstring(zf.read(f"xl/worksheets/sheet{sheet_no}.xml"))
    rows = []
    for row in root.findall(".//m:row", NS):
        values = {}
        for cell in row.findall("m:c", NS):
            col = re.match(r"([A-Z]+)", cell.attrib["r"]).group(1)
            index = 0
            for char in col:
                index = index * 26 + ord(char) - 64
            values[index - 1] = cell_value(cell)
        if values:
            rows.append([values.get(i, "") for i in range(max(values) + 1)])
    return rows


def main() -> None:
    source = Path("taller_data_engineer.xlsx")
    data = Path("datos")
    incremental = data / "incremental"
    data.mkdir(exist_ok=True)
    incremental.mkdir(exist_ok=True)
    with zipfile.ZipFile(source) as zf:
        for sheet_no, (sheet_name, filename) in enumerate(SHEETS.items(), 1):
            rows = read_sheet(zf, sheet_no)
            target = incremental / filename if filename.endswith("jsonl") else data / filename
            if filename.endswith(".csv"):
                with target.open("w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerows(rows)
            else:
                headers, *records = rows
                with target.open("w", encoding="utf-8") as f:
                    for record in records:
                        f.write(json.dumps(dict(zip(headers, record)), ensure_ascii=False) + "\n")
            print(f"{sheet_name}: {len(rows)-1:,} registros -> {target}")


if __name__ == "__main__":
    main()
