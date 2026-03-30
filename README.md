# School Swagger CRUD API

Small FastAPI server that reads data models from `Public School Deliverables - Data Model.csv`, builds SQLite tables, seeds test data, and exposes CRUD endpoints with Swagger UI.

## Local Run

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open:

- Swagger UI: `http://127.0.0.1:8000/docs`
- OpenAPI JSON: `http://127.0.0.1:8000/openapi.json`
- Health: `http://127.0.0.1:8000/health`
- Models metadata: `http://127.0.0.1:8000/models`

## API Pattern

For each inferred model/table:

- `GET /api/{table_name}`
- `POST /api/{table_name}`
- `POST /api/{table_name}/bulk`
- `PUT /api/{table_name}/bulk`
- `GET /api/{table_name}/{record_id}`
- `PUT /api/{table_name}/{record_id}`
- `DELETE /api/{table_name}/{record_id}`

List endpoint query params:

- `page` (default: `1`)
- `page_size` (default: `50`, max: `500`)
- `sort_by` (column name)
- `sort_dir` (`asc` or `desc`)
- Any column name as a filter, e.g. `?status=active&class_id=...`

## Validation Rules

- Unknown fields return `422`
- Missing mandatory fields on create return `422`
- Enum fields must match values defined in the CSV
- Date/time fields accept ISO formatted strings
- Decimal fields are coerced from numeric/string inputs
- Primary key field cannot be changed in update payload

## API Key Auth

Protected endpoints require header:

```text
X-API-Key: <your_api_key>
```

- Set `API_KEY` environment variable in Render.
- If `API_KEY` is not set, auth is bypassed for local development.

`/health` is public.

## Render Free Instance

The repository includes `render.yaml`.

If deploying manually on Render:

- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- Runtime: Python

The first boot creates `school.db` and seeds sample records automatically.
