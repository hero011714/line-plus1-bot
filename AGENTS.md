# Agent Instructions: line_bot

This file provides system context, build/test commands, and coding guidelines for AI agents (like Cursor, Copilot, or CLI agents) operating in this repository. 

## 1. Project Overview
- **Type**: LINE Messaging API Bot.
- **Framework**: FastAPI + Uvicorn.
- **Database**: PostgreSQL (using `psycopg2`).
- **Deployment**: Render.com (via `.github/workflows/deploy.yml` and `render.yaml`).
- **Core Architecture**: The application is currently a monolithic Python script (`main.py`) handling all HTTP routing, business logic, LINE event parsing, and database transactions.

## 2. Build, Run, and Test Commands

### Running Locally
To run the FastAPI server locally in development mode with auto-reload:
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Dependency Management
Install dependencies using `pip`:
```bash
pip install -r requirements.txt
```
Whenever adding a new dependency, ensure it is appended to `requirements.txt`.

### Testing
There is currently no formal test suite. All new features or refactors MUST include unit tests using `pytest`.
When implementing tests, create a `tests/` directory and use standard pytest conventions.

**Run all tests**:
```bash
pytest
```

**Run a single test file**:
```bash
pytest tests/test_feature.py
```

**Run a single specific test (Crucial for isolated debugging)**:
```bash
pytest tests/test_feature.py::test_function_name -v
```

**Run tests with coverage**:
```bash
pytest --cov=.
```

### Linting and Formatting
Currently, there is no enforced linter. However, for any new contributions, agents should format and lint code using `ruff` and type check with `mypy`.
- **Lint**: `ruff check .`
- **Format**: `ruff format .`
- **Type Check**: `mypy main.py`

## 3. Code Style Guidelines

### Naming Conventions
- **Variables & Functions**: Use `snake_case` (e.g., `get_group_id`, `init_tables`).
- **Constants**: Use `UPPER_SNAKE_CASE` (e.g., `CHANNEL_SECRET`, `DATABASE_URL`).
- **Classes**: Use `PascalCase` if introducing any new classes or Pydantic models.
- **Global Variables**: Avoid introducing new global mutable state. Existing global state uses prefix underscores (e.g., `_bot_user_id`, `conn`).

### Imports
Organize imports at the top of the file in the following order:
1. Standard library imports (`os`, `re`, `asyncio`, `datetime`).
2. Third-party imports (`fastapi`, `linebot`, `psycopg2`, `pydantic`).
3. Local module imports (if the project is refactored into multiple files).

### Typing
- While the legacy `main.py` code is largely untyped, **all new functions must include Python type hints**.
- Example: 
  ```python
  def get_user_data(user_id: str, group_id: str) -> dict | None:
      ...
  ```

### Formatting
- **Indentation**: 4 spaces per indentation level. No tabs.
- **Line Length**: Aim for 88-100 characters maximum.
- **Quotes**: Double quotes (`"`) are standard for strings, especially for SQL queries and print statements.

### Error Handling & Logging
- **Current Pattern**: The application currently uses `try...except Exception as e:` blocks and prints errors to stdout using `print()`.
- **Guidelines for New Code**:
  - Catch *specific* exceptions rather than a blanket `Exception` where possible (e.g., `psycopg2.DatabaseError`, `linebot.exceptions.LineBotApiError`).
  - Use the built-in `logging` module (`import logging`) instead of `print()` for new files to allow log-level filtering (`INFO`, `ERROR`, `DEBUG`).
  - Always clean up resources (like database cursors) in a `finally` block or by using a context manager:
    ```python
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(...)
    ```

### Database Access
- The project uses raw SQL queries with `psycopg2`.
- **CRITICAL**: Always use parameterized queries (e.g., `%s`) to prevent SQL injection vulnerabilities.
  - **Good**: `cur.execute("SELECT * FROM users WHERE user_id=%s", (user_id,))`
  - **Bad**: `cur.execute(f"SELECT * FROM users WHERE user_id={user_id}")`
- Be mindful of connections. The app uses `conn.autocommit = True`. Avoid leaving cursors open unnecessarily.

### Framework Usage (FastAPI & LINE SDK)
- Route handlers should use standard FastAPI decorators (`@app.post(...)`).
- LINE events are handled using `WebhookHandler` and processed asynchronously.
- Note that the LINE platform expects rapid responses (200 OK). Long-running tasks should be processed via FastAPI `BackgroundTasks` to avoid webhook timeouts.

## 4. Architecture & Refactoring Guidance
- `main.py` is currently very large. When adding substantial new features, consider extracting logic into separate modules (e.g., `database.py`, `line_handlers.py`, `models.py`).
- Avoid adding more global state. Encapsulate configuration and database connections where possible.
