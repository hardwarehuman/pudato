"""Unit tests for protocol dataclasses."""

from pudato.protocol import Command, DataReference, ExecutionRecord, Result


class TestDataReference:
    """Test DataReference model."""

    def test_basic_creation(self):
        """Test creating a basic DataReference."""
        ref = DataReference(
            ref_type="table",
            location="main.users",
        )
        assert ref.ref_type == "table"
        assert ref.location == "main.users"
        assert ref.format is None
        assert ref.metadata == {}

    def test_with_format_and_metadata(self):
        """Test DataReference with all fields."""
        ref = DataReference(
            ref_type="file",
            location="s3://bucket/path/data.parquet",
            format="application/parquet",
            metadata={"size_bytes": 1024, "row_count": 100},
        )
        assert ref.ref_type == "file"
        assert ref.location == "s3://bucket/path/data.parquet"
        assert ref.format == "application/parquet"
        assert ref.metadata["size_bytes"] == 1024

    def test_serialization(self):
        """Test DataReference serialization to dict."""
        ref = DataReference(
            ref_type="stream",
            location="kafka://topic/partition",
        )
        data = ref.model_dump()
        assert data["ref_type"] == "stream"
        assert data["location"] == "kafka://topic/partition"

    def test_various_ref_types(self):
        """Test different ref_type values."""
        types = ["table", "file", "stream", "model", "api", "view"]
        for ref_type in types:
            ref = DataReference(ref_type=ref_type, location="test")
            assert ref.ref_type == ref_type


class TestExecutionRecord:
    """Test ExecutionRecord model and factory methods."""

    def test_sql_factory(self):
        """Test ExecutionRecord.sql factory method."""
        record = ExecutionRecord.sql(
            statements=["SELECT * FROM users", "SELECT * FROM orders"],
            dialect="postgresql",
            parameters={"limit": 100},
        )
        assert record.execution_type == "sql"
        assert record.details["dialect"] == "postgresql"
        assert record.details["statements"] == ["SELECT * FROM users", "SELECT * FROM orders"]
        assert record.details["parameters"] == {"limit": 100}

    def test_sql_factory_defaults(self):
        """Test ExecutionRecord.sql with defaults."""
        record = ExecutionRecord.sql(statements=["SELECT 1"])
        assert record.details["dialect"] == "duckdb"
        assert record.details["statements"] == ["SELECT 1"]

    def test_python_factory(self):
        """Test ExecutionRecord.python factory method."""
        record = ExecutionRecord.python(
            module="myapp.transforms",
            function="process_data",
            version="1.2.3",
            args=["input.csv"],
            kwargs={"output_format": "parquet"},
        )
        assert record.execution_type == "python"
        assert record.details["module"] == "myapp.transforms"
        assert record.details["function"] == "process_data"
        assert record.details["version"] == "1.2.3"
        assert record.details["args"] == ["input.csv"]
        assert record.details["kwargs"] == {"output_format": "parquet"}

    def test_dbt_factory(self):
        """Test ExecutionRecord.dbt factory method."""
        record = ExecutionRecord.dbt(
            command="build",
            models=["stg_users", "stg_orders"],
            project_dir="/path/to/dbt",
            target="prod",
        )
        assert record.execution_type == "dbt"
        assert record.details["command"] == "build"
        assert record.details["models"] == ["stg_users", "stg_orders"]
        assert record.details["project_dir"] == "/path/to/dbt"
        assert record.details["target"] == "prod"

    def test_dbt_factory_minimal(self):
        """Test ExecutionRecord.dbt with minimal args."""
        record = ExecutionRecord.dbt(command="run")
        assert record.details["command"] == "run"
        # models defaults to empty list when not provided
        assert record.details["models"] == [] or record.details["models"] is None
        assert record.details["target"] is None

    def test_custom_execution_type(self):
        """Test creating custom execution types."""
        record = ExecutionRecord(
            execution_type="spark",
            details={
                "app_name": "etl_job",
                "master": "yarn",
                "executor_memory": "4g",
            },
        )
        assert record.execution_type == "spark"
        assert record.details["app_name"] == "etl_job"

    def test_serialization(self):
        """Test ExecutionRecord serialization."""
        record = ExecutionRecord.sql(statements=["SELECT 1"])
        data = record.model_dump()
        assert data["execution_type"] == "sql"
        assert "statements" in data["details"]


class TestResultWithLineage:
    """Test Result model with lineage fields."""

    def test_result_with_inputs(self):
        """Test Result with inputs."""
        inputs = [
            DataReference(ref_type="table", location="raw.users"),
            DataReference(ref_type="table", location="raw.orders"),
        ]
        result = Result(
            status="success",
            data={"rows": 100},
            inputs=inputs,
            correlation_id="test-123",
        )
        assert len(result.inputs) == 2
        assert result.inputs[0].location == "raw.users"

    def test_result_with_outputs(self):
        """Test Result with outputs."""
        outputs = [
            DataReference(ref_type="table", location="main.dim_users"),
        ]
        result = Result(
            status="success",
            data={"created": True},
            outputs=outputs,
            correlation_id="test-123",
        )
        assert len(result.outputs) == 1
        assert result.outputs[0].location == "main.dim_users"

    def test_result_with_executions(self):
        """Test Result with executions."""
        executions = [
            ExecutionRecord.sql(statements=["CREATE TABLE...", "INSERT INTO..."]),
        ]
        result = Result(
            status="success",
            data={"executed": True},
            executions=executions,
            correlation_id="test-123",
        )
        assert len(result.executions) == 1
        assert result.executions[0].execution_type == "sql"

    def test_result_defaults_empty_lineage(self):
        """Test that Result defaults lineage to empty lists."""
        result = Result(
            status="success",
            data={"done": True},
            correlation_id="test-123",
        )
        assert result.inputs == []
        assert result.outputs == []
        assert result.executions == []

    def test_result_serialization_with_lineage(self):
        """Test Result serialization includes lineage."""
        result = Result(
            status="success",
            data={"value": 42},
            inputs=[DataReference(ref_type="file", location="input.csv")],
            outputs=[DataReference(ref_type="file", location="output.parquet")],
            executions=[ExecutionRecord.python(module="app", function="transform")],
            correlation_id="test-123",
        )
        json_str = result.to_json()
        assert "inputs" in json_str
        assert "outputs" in json_str
        assert "executions" in json_str
        assert "input.csv" in json_str
        assert "output.parquet" in json_str

    def test_result_deserialization_with_lineage(self):
        """Test Result deserialization restores lineage."""
        original = Result(
            status="success",
            data={"test": True},
            inputs=[DataReference(ref_type="table", location="src")],
            outputs=[DataReference(ref_type="table", location="dst")],
            executions=[ExecutionRecord.sql(statements=["SELECT"])],
            correlation_id="test-123",
        )
        json_str = original.to_json()
        restored = Result.from_json(json_str)

        assert len(restored.inputs) == 1
        assert restored.inputs[0].location == "src"
        assert len(restored.outputs) == 1
        assert restored.outputs[0].location == "dst"
        assert len(restored.executions) == 1
        assert restored.executions[0].execution_type == "sql"


class TestCommand:
    """Test Command model."""

    def test_command_creation(self):
        """Test basic Command creation."""
        cmd = Command(
            type="query",
            action="execute",
            payload={"sql": "SELECT 1"},
        )
        assert cmd.type == "query"
        assert cmd.action == "execute"
        assert cmd.payload["sql"] == "SELECT 1"
        assert cmd.correlation_id  # auto-generated

    def test_command_with_metadata(self):
        """Test Command with metadata."""
        cmd = Command(
            type="transform",
            action="build",
            payload={"select": "stg_users"},
            metadata={"logic_version": "v1.0.0", "execution_id": "run-001"},
        )
        assert cmd.metadata["logic_version"] == "v1.0.0"

    def test_command_serialization(self):
        """Test Command serialization roundtrip."""
        original = Command(
            type="storage",
            action="put_object",
            payload={"container": "bucket", "path": "key"},
        )
        json_str = original.to_json()
        restored = Command.from_json(json_str)
        assert restored.type == original.type
        assert restored.action == original.action
        assert restored.payload == original.payload
