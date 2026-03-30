from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Generator, Optional, cast
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Column,
    Date,
    DateTime,
    Enum,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    Time,
    create_engine,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .schema_loader import ColumnSchema, TableSchema, load_table_schemas


DATABASE_URL = "sqlite:///./school.db"
CSV_MODEL_FILE = (
    Path(__file__).resolve().parent.parent
    / "School Deliverables - Data Model.csv"
)


engine: Engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
metadata = MetaData()


@dataclass
class RuntimeModel:
    schema: TableSchema
    table: Table
    pk_column: str


RUNTIME_MODELS: Dict[str, RuntimeModel] = {}


def _parse_length(data_type: str) -> Optional[int]:
    match = re.fullmatch(r"(?:varchar|char)\((\d+)\)", data_type)
    if not match:
        return None
    return int(match.group(1))


def _parse_decimal(data_type: str) -> Optional[tuple[int, int]]:
    match = re.fullmatch(r"decimal\((\d+),(\d+)\)", data_type)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _column_type(table_name: str, column: ColumnSchema):
    data_type = column.data_type.strip().lower()

    if data_type == "enum":
        values = column.enum_values or ["value"]
        return Enum(
            *values,
            name=f"{table_name}_{column.name}_enum",
            native_enum=False,
        )

    length = _parse_length(data_type)
    if length is not None:
        return String(length)

    dec = _parse_decimal(data_type)
    if dec is not None:
        precision, scale = dec
        return Numeric(precision=precision, scale=scale)

    mapping = {
        "int": Integer,
        "tinyint": Integer,
        "smallint": Integer,
        "bigint": Integer,
        "date": Date,
        "datetime": DateTime,
        "time": Time,
        "text": Text,
        "longtext": Text,
        "json": JSON,
    }

    type_cls = mapping.get(data_type)
    if type_cls:
        return type_cls()

    return String(255)


def _parse_default(column: ColumnSchema) -> Any:
    if column.default_value is None or column.default_value == "":
        return None

    data_type = column.data_type.strip().lower()
    raw = column.default_value

    if data_type in {"int", "tinyint", "smallint", "bigint"}:
        try:
            return int(raw)
        except ValueError:
            return None

    if data_type.startswith("decimal"):
        try:
            return Decimal(raw)
        except Exception:
            return None

    if data_type == "date":
        try:
            return date.fromisoformat(raw)
        except ValueError:
            return None

    if data_type == "datetime":
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            return None

    if data_type == "time":
        try:
            return time.fromisoformat(raw)
        except ValueError:
            return None

    if data_type == "json":
        return {}

    return raw


def _build_table(schema: TableSchema) -> Table:
    columns = []
    for col in schema.columns:
        default = _parse_default(col)
        kwargs: Dict[str, Any] = {
            "nullable": col.condition != "mandatory",
            "primary_key": col.key_type == "primary_key",
            "unique": col.key_type == "unique_key",
        }
        if default is not None:
            kwargs["default"] = default

        column_type = cast(Any, _column_type(schema.name, col))
        columns.append(Column(col.name, column_type, **kwargs))

    return Table(schema.name, metadata, *columns)


def _value_for_column(column: ColumnSchema) -> Any:
    default = _parse_default(column)
    if default is not None:
        return default

    data_type = column.data_type.strip().lower()

    if column.key_type == "primary_key" and column.name.endswith("_id"):
        return str(uuid4())

    if column.key_type == "foreign_key" and column.name.endswith("_id"):
        return str(uuid4())

    if data_type == "enum":
        return column.enum_values[0] if column.enum_values else "value"

    if data_type in {"int", "tinyint", "smallint", "bigint"}:
        return 1

    if data_type.startswith("decimal"):
        return Decimal("100.00")

    if data_type == "date":
        return date.today()

    if data_type == "datetime":
        return datetime.utcnow()

    if data_type == "time":
        return datetime.utcnow().time().replace(microsecond=0)

    if data_type == "json":
        return {"sample": True}

    if column.name.endswith("_id"):
        return str(uuid4())

    return f"sample_{column.name}"


def _seed_one_row(schema: TableSchema) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for col in schema.columns:
        if (
            col.condition == "mandatory"
            or col.key_type in {"primary_key", "foreign_key"}
            or col.default_value
        ):
            payload[col.name] = _value_for_column(col)
    return payload


def _make_distinct_value(current: Any, data_type: str) -> Any:
    normalized = data_type.strip().lower()

    if isinstance(current, str):
        return f"{current}_{uuid4().hex[:8]}"

    if normalized in {"int", "tinyint", "smallint", "bigint"}:
        return int(current) + 1

    if normalized.startswith("decimal"):
        return Decimal(str(current)) + Decimal("1")

    if normalized == "date" and isinstance(current, date):
        return current + timedelta(days=1)

    if normalized == "datetime" and isinstance(current, datetime):
        return current + timedelta(seconds=1)

    if normalized == "time" and isinstance(current, time):
        base = datetime.combine(date.today(), current)
        return (base + timedelta(minutes=1)).time().replace(microsecond=0)

    return str(uuid4())


def initialize_runtime_models() -> Dict[str, RuntimeModel]:
    if RUNTIME_MODELS:
        return RUNTIME_MODELS

    schemas = load_table_schemas(CSV_MODEL_FILE)

    for schema in schemas:
        if not schema.primary_key:
            continue
        table = _build_table(schema)
        RUNTIME_MODELS[schema.name] = RuntimeModel(
            schema=schema,
            table=table,
            pk_column=schema.primary_key,
        )

    metadata.create_all(bind=engine)
    return RUNTIME_MODELS


def seed_test_data() -> None:
    with SessionLocal() as db:
        for model in RUNTIME_MODELS.values():
            existing = db.execute(model.table.select().limit(1)).first()
            if existing is not None:
                continue

            first_row = _seed_one_row(model.schema)
            second_row = _seed_one_row(model.schema)

            for col in model.schema.columns:
                if col.key_type not in {"primary_key", "unique_key"}:
                    continue
                if col.name not in second_row:
                    continue

                second_row[col.name] = _make_distinct_value(
                    current=second_row[col.name],
                    data_type=col.data_type,
                )

            db.execute(model.table.insert().values([first_row, second_row]))

        db.commit()


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def coerce_pk_value(column: ColumnSchema, raw_value: str) -> Any:
    data_type = column.data_type.strip().lower()
    if data_type in {"int", "tinyint", "smallint", "bigint"}:
        return int(raw_value)
    return raw_value
