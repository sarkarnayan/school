from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class ColumnSchema:
    name: str
    data_type: str
    condition: str
    default_value: Optional[str]
    enum_values: List[str]
    key_type: str


@dataclass
class TableSchema:
    name: str
    columns: List[ColumnSchema]

    @property
    def primary_key(self) -> Optional[str]:
        for column in self.columns:
            if column.key_type == "primary_key":
                return column.name
        return None


def _clean(value: Optional[str]) -> str:
    return (value or "").strip()


def _pluralize(word: str) -> str:
    if word.endswith("y") and len(word) > 1 and word[-2] not in "aeiou":
        return f"{word[:-1]}ies"
    if word.endswith(("s", "x", "z", "ch", "sh")):
        return f"{word}es"
    return f"{word}s"


def _normalize_identifier(value: str) -> str:
    return value.strip().lower().replace(" ", "_")


def _infer_table_name(rows: List[ColumnSchema], index: int) -> str:
    pk_column = next((c.name for c in rows if c.key_type == "primary_key"), "")

    if pk_column.endswith("_id"):
        base = pk_column[:-3]
        if base:
            return _pluralize(base)

    if pk_column == "id":
        fk_bases = []
        for column in rows:
            if (
                column.key_type == "foreign_key"
                and column.name.endswith("_id")
            ):
                fk_bases.append(column.name[:-3])
        if len(fk_bases) >= 2:
            composite = "_".join(dict.fromkeys(fk_bases[:2]))
            return _pluralize(composite)

    return f"model_{index}"


def _is_block_header(row: Dict[str, str]) -> bool:
    return (
        _clean(row.get("column_name", "")).lower() == "column_name"
        and _clean(row.get("data_type", "")).lower() == "data_type"
    )


def _row_is_empty(row: Dict[str, str]) -> bool:
    default_value = _clean(row.get("default_value", ""))
    default_values = _clean(row.get("default_values", ""))

    fields = [
        _clean(row.get("column_name", "")),
        _clean(row.get("data_type", "")),
        _clean(row.get("condition", "")),
        default_value or default_values,
        _clean(row.get("enum_values", "")),
        _clean(row.get("key_type", "")),
    ]
    return all(not field for field in fields)


def _to_column_schema(row: Dict[str, str]) -> Optional[ColumnSchema]:
    name = _normalize_identifier(_clean(row.get("column_name")))
    if not name or name == "column_name":
        return None

    data_type = _clean(row.get("data_type", "")).strip('"').lower()
    condition = _clean(row.get("condition", "")).lower() or "optional"
    default_value = (
        _clean(row.get("default_value", ""))
        or _clean(row.get("default_values", ""))
    )
    enum_raw = _clean(row.get("enum_values", ""))
    key_type = _clean(row.get("key_type", "")).lower()

    enum_values = (
        [item.strip() for item in enum_raw.split(",") if item.strip()]
        if enum_raw
        else []
    )

    return ColumnSchema(
        name=name,
        data_type=data_type,
        condition=condition,
        default_value=default_value or None,
        enum_values=enum_values,
        key_type=key_type,
    )


def load_table_schemas(csv_path: Path) -> List[TableSchema]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV schema file not found: {csv_path}")

    tables: List[TableSchema] = []
    current_rows: List[ColumnSchema] = []

    with csv_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            if _is_block_header(row):
                if current_rows:
                    table_name = _infer_table_name(
                        current_rows,
                        len(tables) + 1,
                    )
                    tables.append(
                        TableSchema(name=table_name, columns=current_rows)
                    )
                    current_rows = []
                continue

            if _row_is_empty(row):
                continue

            column = _to_column_schema(row)
            if column:
                current_rows.append(column)

    if current_rows:
        table_name = _infer_table_name(current_rows, len(tables) + 1)
        tables.append(TableSchema(name=table_name, columns=current_rows))

    deduped: List[TableSchema] = []
    seen: Dict[str, int] = {}
    for table in tables:
        if table.name not in seen:
            seen[table.name] = 1
            deduped.append(table)
            continue

        seen[table.name] += 1
        deduped.append(
            TableSchema(
                name=f"{table.name}_{seen[table.name]}",
                columns=table.columns,
            )
        )

    return deduped
