from __future__ import annotations

import json
import math
import os
import re
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, List, Optional, cast

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi import Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy import and_, asc, desc, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .database import (
    RUNTIME_MODELS,
    coerce_pk_value,
    get_db,
    initialize_runtime_models,
    seed_test_data,
)
from .schema_loader import ColumnSchema


app = FastAPI(
    title="School Data Models API",
    description=(
        "CRUD API generated from School Public School Deliverables "
        "CSV schema."
    ),
    version="1.0.0",
)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(
    api_key: Optional[str] = Security(API_KEY_HEADER),
) -> str:
    configured_key = os.getenv("API_KEY", "").strip()
    if not configured_key:
        # Local/dev fallback when API_KEY is not configured.
        return ""

    if not api_key or api_key != configured_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )

    return api_key


AUTH_DEPENDENCIES = [Depends(require_api_key)]


def _serialize(record: Dict[str, Any]) -> Dict[str, Any]:
    serialized: Dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, Decimal):
            serialized[key] = float(value)
        else:
            serialized[key] = value
    return serialized


def _is_decimal_type(data_type: str) -> bool:
    return bool(re.fullmatch(r"decimal\(\d+,\d+\)", data_type))


def _coerce_value(column: ColumnSchema, value: Any) -> Any:
    data_type = column.data_type.strip().lower()

    if value is None:
        return None

    if data_type == "enum":
        if not isinstance(value, str):
            raise ValueError("must be a string")
        if column.enum_values and value not in column.enum_values:
            allowed = ", ".join(column.enum_values)
            raise ValueError(f"must be one of: {allowed}")
        return value

    if data_type in {"int", "tinyint", "smallint", "bigint"}:
        return int(value)

    if _is_decimal_type(data_type):
        return Decimal(str(value))

    if data_type == "date":
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise ValueError("must be ISO date string")

    if data_type == "datetime":
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        raise ValueError("must be ISO datetime string")

    if data_type == "time":
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            return time.fromisoformat(value)
        raise ValueError("must be ISO time string")

    if data_type == "json":
        if isinstance(value, str):
            return json.loads(value)
        if isinstance(value, (dict, list, int, float, bool)):
            return value
        raise ValueError("must be valid JSON")

    return str(value)


def _validate_payload(
    payload: Dict[str, Any],
    columns: List[ColumnSchema],
    is_update: bool,
    pk_column_name: str,
) -> Dict[str, Any]:
    column_map = {col.name: col for col in columns}
    allowed_fields = set(column_map)

    unknown_fields = [
        field for field in payload if field not in allowed_fields
    ]
    if unknown_fields:
        names = ", ".join(sorted(unknown_fields))
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown fields: {names}",
        )

    if is_update and pk_column_name in payload:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Primary key '{pk_column_name}' cannot be updated",
        )

    if not is_update:
        missing_required = []
        for col in columns:
            if col.condition != "mandatory":
                continue
            if col.name in payload:
                continue
            if col.default_value not in (None, ""):
                continue
            missing_required.append(col.name)

        if missing_required:
            names = ", ".join(missing_required)
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Missing required fields: {names}",
            )

    validated: Dict[str, Any] = {}
    for field, value in payload.items():
        column = column_map[field]

        if value is None:
            if column.condition == "mandatory":
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Field '{field}' cannot be null",
                )
            validated[field] = None
            continue

        try:
            validated[field] = _coerce_value(column, value)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid '{field}': {exc}",
            ) from exc

    return validated


def _extract_filters(
    request: Request,
    columns: List[ColumnSchema],
) -> Dict[str, str]:
    reserved = {"page", "page_size", "sort_by", "sort_dir"}
    column_map = {col.name: col for col in columns}
    filters: Dict[str, str] = {}

    for key, value in request.query_params.items():
        if key in reserved:
            continue

        if key not in column_map:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unknown filter field: {key}",
            )

        filters[key] = value

    return filters


def _build_filter_conditions(
    filters: Dict[str, str],
    table: Any,
    columns: List[ColumnSchema],
) -> List[Any]:
    column_map = {col.name: col for col in columns}
    conditions: List[Any] = []

    for field, raw_value in filters.items():
        col_schema = column_map[field]
        try:
            coerced = _coerce_value(col_schema, raw_value)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid filter '{field}': {exc}",
            ) from exc

        conditions.append(table.c[field] == coerced)

    return conditions


def _fetch_one(db: Session, table: Any, pk_name: str, pk_value: Any) -> Any:
    return (
        db.execute(table.select().where(table.c[pk_name] == pk_value))
        .mappings()
        .first()
    )


@app.on_event("startup")
def startup_event() -> None:
    initialize_runtime_models()
    seed_test_data()


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/models", dependencies=AUTH_DEPENDENCIES)
def list_models() -> List[Dict[str, Any]]:
    models = []
    for runtime in RUNTIME_MODELS.values():
        models.append(
            {
                "table": runtime.schema.name,
                "primary_key": runtime.pk_column,
                "columns": [
                    {
                        "name": c.name,
                        "type": c.data_type,
                        "condition": c.condition,
                        "key_type": c.key_type,
                    }
                    for c in runtime.schema.columns
                ],
            }
        )
    return models


def _register_routes(table_name: str) -> None:
    runtime = RUNTIME_MODELS[table_name]
    table = runtime.table
    pk_column_name = runtime.pk_column
    pk_schema = next(
        col for col in runtime.schema.columns if col.name == pk_column_name
    )

    async def list_rows(
        request: Request,
        page: int = Query(default=1, ge=1),
        page_size: int = Query(default=50, ge=1, le=500),
        sort_by: Optional[str] = Query(default=None),
        sort_dir: str = Query(default="asc"),
        db: Session = Depends(get_db),
    ):
        filters = _extract_filters(request, runtime.schema.columns)
        conditions = _build_filter_conditions(
            filters=filters,
            table=table,
            columns=runtime.schema.columns,
        )

        if sort_by:
            known_columns = {col.name for col in runtime.schema.columns}
            if sort_by not in known_columns:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Unknown sort field: {sort_by}",
                )
            sort_column = table.c[sort_by]
        else:
            sort_column = table.c[pk_column_name]

        sort_direction = sort_dir.lower().strip()
        if sort_direction not in {"asc", "desc"}:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="sort_dir must be 'asc' or 'desc'",
            )

        order_clause = (
            asc(sort_column)
            if sort_direction == "asc"
            else desc(sort_column)
        )

        filtered_query = table.select()
        count_query = select(func.count()).select_from(table)
        if conditions:
            filtered_query = filtered_query.where(and_(*conditions))
            count_query = count_query.where(and_(*conditions))

        offset = (page - 1) * page_size
        rows = (
            db.execute(
                filtered_query.order_by(order_clause)
                .limit(page_size)
                .offset(offset)
            )
            .mappings()
            .all()
        )
        total = db.execute(count_query).scalar_one()
        total_pages = math.ceil(total / page_size) if total else 0

        return {
            "items": [_serialize(dict(row)) for row in rows],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": total_pages,
            },
            "filters": filters,
            "sort": {
                "sort_by": sort_by or pk_column_name,
                "sort_dir": sort_direction,
            },
        }

    async def create_row(
        payload: Dict[str, Any] = Body(...),
        db: Session = Depends(get_db),
    ):
        validated_payload = _validate_payload(
            payload=payload,
            columns=runtime.schema.columns,
            is_update=False,
            pk_column_name=pk_column_name,
        )

        try:
            db.execute(table.insert().values(**validated_payload))
            db.commit()
        except IntegrityError as exc:
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc.orig),
            ) from exc

        pk_value = validated_payload.get(pk_column_name)
        if pk_value is None:
            return {"message": "created"}

        row = _fetch_one(db, table, pk_column_name, pk_value)
        return _serialize(dict(row)) if row else {"message": "created"}

    async def bulk_create_rows(
        payload: List[Dict[str, Any]] = Body(...),
        db: Session = Depends(get_db),
    ):
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Payload must contain at least one record",
            )

        validated_rows: List[Dict[str, Any]] = []
        for index, row in enumerate(payload):
            try:
                validated = _validate_payload(
                    payload=row,
                    columns=runtime.schema.columns,
                    is_update=False,
                    pk_column_name=pk_column_name,
                )
            except HTTPException as exc:
                detail = (
                    f"bulk insert item {index} failed: "
                    f"{exc.detail}"
                )
                raise HTTPException(
                    status_code=exc.status_code,
                    detail=detail,
                ) from exc

            validated_rows.append(validated)

        try:
            db.execute(table.insert(), validated_rows)
            db.commit()
        except IntegrityError as exc:
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc.orig),
            ) from exc

        return {
            "message": "bulk insert completed",
            "inserted": len(validated_rows),
        }

    async def get_row(record_id: str, db: Session = Depends(get_db)):
        parsed_id = coerce_pk_value(pk_schema, record_id)
        row = _fetch_one(db, table, pk_column_name, parsed_id)
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Record not found",
            )
        return _serialize(dict(row))

    async def update_row(
        record_id: str,
        payload: Dict[str, Any] = Body(...),
        db: Session = Depends(get_db),
    ):
        parsed_id = coerce_pk_value(pk_schema, record_id)
        existing = _fetch_one(db, table, pk_column_name, parsed_id)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Record not found",
            )

        validated_payload = _validate_payload(
            payload=payload,
            columns=runtime.schema.columns,
            is_update=True,
            pk_column_name=pk_column_name,
        )
        if not validated_payload:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="No updatable fields provided",
            )

        try:
            db.execute(
                table.update()
                .where(table.c[pk_column_name] == parsed_id)
                .values(**validated_payload)
            )
            db.commit()
        except IntegrityError as exc:
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc.orig),
            ) from exc

        updated = _fetch_one(db, table, pk_column_name, parsed_id)
        return _serialize(dict(updated)) if updated else {"message": "updated"}

    async def bulk_update_rows(
        payload: List[Dict[str, Any]] = Body(...),
        db: Session = Depends(get_db),
    ):
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Payload must contain at least one record",
            )

        updated_count = 0
        for index, row in enumerate(payload):
            if pk_column_name not in row:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"bulk update item {index} missing primary key "
                        f"'{pk_column_name}'"
                    ),
                )

            raw_pk = row[pk_column_name]
            parsed_pk = coerce_pk_value(pk_schema, str(raw_pk))
            existing = _fetch_one(db, table, pk_column_name, parsed_pk)
            if not existing:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=(
                        f"bulk update item {index} not found for "
                        f"{pk_column_name}={raw_pk}"
                    ),
                )

            update_payload = {
                key: value
                for key, value in row.items()
                if key != pk_column_name
            }
            validated_update = _validate_payload(
                payload=update_payload,
                columns=runtime.schema.columns,
                is_update=True,
                pk_column_name=pk_column_name,
            )
            if not validated_update:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"bulk update item {index} has no fields to update"
                    ),
                )

            try:
                db.execute(
                    table.update()
                    .where(table.c[pk_column_name] == parsed_pk)
                    .values(**validated_update)
                )
            except IntegrityError as exc:
                db.rollback()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"bulk update item {index} failed: "
                        f"{exc.orig}"
                    ),
                ) from exc

            updated_count += 1

        db.commit()
        return {
            "message": "bulk update completed",
            "updated": updated_count,
        }

    async def delete_row(record_id: str, db: Session = Depends(get_db)):
        parsed_id = coerce_pk_value(pk_schema, record_id)
        existing = _fetch_one(db, table, pk_column_name, parsed_id)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Record not found",
            )

        db.execute(table.delete().where(table.c[pk_column_name] == parsed_id))
        db.commit()
        return {"message": "deleted"}

    tags = cast(List[Any], [f"{table_name}"])

    app.add_api_route(
        f"/api/{table_name}",
        list_rows,
        methods=["GET"],
        dependencies=AUTH_DEPENDENCIES,
        tags=tags,
        summary=f"List {table_name}",
        name=f"list_{table_name}",
    )
    app.add_api_route(
        f"/api/{table_name}",
        create_row,
        methods=["POST"],
        dependencies=AUTH_DEPENDENCIES,
        tags=tags,
        summary=f"Create {table_name} record",
        name=f"create_{table_name}",
    )
    app.add_api_route(
        f"/api/{table_name}/bulk",
        bulk_create_rows,
        methods=["POST"],
        dependencies=AUTH_DEPENDENCIES,
        tags=tags,
        summary=f"Bulk create {table_name} records",
        name=f"bulk_create_{table_name}",
    )
    app.add_api_route(
        f"/api/{table_name}/bulk",
        bulk_update_rows,
        methods=["PUT"],
        dependencies=AUTH_DEPENDENCIES,
        tags=tags,
        summary=f"Bulk update {table_name} records",
        name=f"bulk_update_{table_name}",
    )
    app.add_api_route(
        f"/api/{table_name}/{{record_id}}",
        get_row,
        methods=["GET"],
        dependencies=AUTH_DEPENDENCIES,
        tags=tags,
        summary=f"Get {table_name} record",
        name=f"get_{table_name}",
    )
    app.add_api_route(
        f"/api/{table_name}/{{record_id}}",
        update_row,
        methods=["PUT"],
        dependencies=AUTH_DEPENDENCIES,
        tags=tags,
        summary=f"Update {table_name} record",
        name=f"update_{table_name}",
    )
    app.add_api_route(
        f"/api/{table_name}/{{record_id}}",
        delete_row,
        methods=["DELETE"],
        dependencies=AUTH_DEPENDENCIES,
        tags=tags,
        summary=f"Delete {table_name} record",
        name=f"delete_{table_name}",
    )


# Register API paths immediately so routes appear in OpenAPI / Swagger.
initialize_runtime_models()
for _table_name in list(RUNTIME_MODELS):
    _register_routes(_table_name)
