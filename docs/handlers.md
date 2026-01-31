# Handlers

Handlers are the core abstraction for service operations. Each handler receives `Command` messages and returns `Result` messages.

**Source**: `src/pudato/handlers/`

## Handler Protocol

```python
class Handler(Protocol):
    def handle(self, command: Command) -> Result: ...
    def supported_actions(self) -> list[str]: ...
```

## Available Handlers

| Handler | Backend(s) | Actions | Source |
|---------|------------|---------|--------|
| `StorageHandler` | S3Backend | put_object, get_object, list_objects, delete_object | `handlers/storage.py` |
| `QueryHandler` | DuckDBBackend | execute_sql | `handlers/query.py` |
| `TransformHandler` | DbtBackend | run, build, test, seed | `handlers/transform.py` |
| `TableHandler` | DuckDBTableBackend | create, insert, query | `handlers/table.py` |
| `CatalogHandler` | InMemory, FileBacked | register, get, list, search | `handlers/catalog.py` |
| `RegistryHandler` | InMemory, PostgreSQL | create_job, update_job, add_step, query_lineage | `handlers/registry.py` |

## Handler Pattern

Each handler follows this pattern:

```python
class StorageHandler(Handler):
    def __init__(self, backend: StorageBackend):
        self.backend = backend

    def handle(self, command: Command) -> Result:
        match command.action:
            case "put_object":
                return self._put_object(command)
            case "get_object":
                return self._get_object(command)
            case _:
                return Result.error(
                    correlation_id=command.correlation_id,
                    errors=[f"Unknown action: {command.action}"]
                )

    def supported_actions(self) -> list[str]:
        return ["put_object", "get_object", "list_objects", "delete_object"]
```

## Backend Abstraction

Handlers delegate to backends for cloud-specific implementation:

```python
class StorageBackend(Protocol):
    def put_object(self, container: str, path: str, data: bytes) -> dict: ...
    def get_object(self, container: str, path: str) -> bytes: ...

class S3Backend(StorageBackend):
    def __init__(self, s3_client):
        self.s3 = s3_client

    def put_object(self, container: str, path: str, data: bytes) -> dict:
        response = self.s3.put_object(Bucket=container, Key=path, Body=data)
        return {"etag": response["ETag"]}
```

**Terminology**: Use `container`/`path` (not `bucket`/`key`) for cloud portability.

## Lineage on Results

Handlers can attach lineage data to Results:

```python
result = Result.success(
    correlation_id=command.correlation_id,
    data={"rows_affected": 100},
    duration_ms=45,
    inputs=[DataReference(ref_type="table", location="raw.source")],
    outputs=[DataReference(ref_type="table", location="staging.cleaned")],
    executions=[ExecutionRecord(execution_type="sql", details={...})],
)
```

See `docs/schema.md` for how lineage is persisted.

## Adding a New Handler

1. Create backend protocol in `src/pudato/backends/myservice.py`
2. Implement backend(s) (e.g., `LocalBackend`, `AWSBackend`)
3. Create handler in `src/pudato/handlers/myservice.py`
4. Register in `src/pudato/runtime/lambda_handler.py`:
   ```python
   HANDLER_FACTORIES["myservice"] = lambda: MyServiceHandler(MyBackend())
   ```
5. Add SNS topic + SQS queue in `terraform/modules/messaging/`
6. Add integration tests in `tests/integration/test_myservice.py`
