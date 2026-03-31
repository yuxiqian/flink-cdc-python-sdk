"""
Example usage of Flink CDC Python SDK.

This example demonstrates how to:
1. Create an environment with Flink CDC version, Flink home, and JARs
2. Create a pipeline using the fluent API (pure YAML generator)
3. Generate YAML configuration
4. Execute the pipeline via Env.execute(pipeline)
"""

from flink_cdc_python import (
    Env,
    Pipeline,
)

# =============================================================================
# Example 1: Environment Setup
# =============================================================================

# Env manages all environment configuration:
# - Flink CDC version and auto-download
# - Flink home (user's installation)
# - Additional JAR dependencies

env = Env.create(
    flink_cdc_version="3.3.0",
    flink_home="/path/to/flink-2.0.0",
    jars=[
        "/path/to/mysql-connector-java-8.0.27.jar",
    ],
)

print("Environment Configuration:")
print(f"  Flink CDC Version: {env.flink_cdc_version}")
print(f"  Flink Home: {env.flink_home}")
print(f"  Flink CDC Path: {env.flink_cdc_path}")
print(f"  JARs: {env.jars}")

# Add connectors (downloaded from Maven Central)
# Note: setup() is called automatically when needed
# env.add_connector("flink-cdc-pipeline-connector-mysql")
# env.add_connector("flink-cdc-pipeline-connector-doris")

# List all JARs (env.jars + connectors)
# all_jars = env.list_jars()
# print(f"All JARs: {all_jars}")

# =============================================================================
# Example 2: Basic Pipeline YAML Generation (no env dependency)
# =============================================================================

# Pipeline is a pure YAML generator - no environment dependency
pipeline = (
    Pipeline.create(
        "Sync MySQL Database to Doris",
        parallelism=2,
        schema_change_behavior="evolve",
    )
    .from_source(
        "mysql",
        hostname="localhost",
        port=3306,
        username="root",
        password="123456",
        tables=r"app_db\..*",
    )
    .transform(
        r"app_db.orders",
        projection="id, order_id, UPPER(product_name) as product_name",
        filtering="id > 10 AND order_id > 100",
    )
    .route("app_db.orders", "ods_db.ods_orders")
    .route("app_db.shipments", "ods_db.ods_shipments")
    .route(r"app_db\..*", "ods_db.others")
    .to_sink("doris", fenodes="127.0.0.1:8030", username="root", password="123456")
)

print("\n" + "=" * 60)
print("Generated Pipeline YAML:")
print("=" * 60)
print(pipeline.to_yaml())

# =============================================================================
# Example 3: Pipeline with Java UDFs and AI Models
# =============================================================================

pipeline_with_udf = (
    Pipeline.create("MySQL to Doris with UDF", parallelism=2)
    .from_source(
        "mysql",
        hostname="localhost",
        port=3306,
        username="root",
        password="123456",
        tables=r"app_db\..*",
    )
    .with_java_udf("addone", "com.example.functions.AddOneFunctionClass")
    .with_ai_model(
        "CHAT",
        "OpenAIChatModel",
        openai_model="gpt-3.5-turbo",
        openai_host="https://api.openai.com/",
        openai_apikey="your-api-key",
        openai_chat_prompt="Please summarize this",
    )
    .to_sink("doris", fenodes="127.0.0.1:8030", username="root", password="")
)

print("\n" + "=" * 60)
print("Pipeline with UDF and AI Model:")
print("=" * 60)
print(pipeline_with_udf.to_yaml())

# =============================================================================
# Example 4: Advanced Pipeline
# =============================================================================

advanced_pipeline = (
    Pipeline.create(
        "Advanced Pipeline",
        parallelism=4,
        local_time_zone="America/Los_Angeles",
        execution_runtime_mode="STREAMING",
        route_mode="ALL_MATCH",
    )
    .from_source(
        "mysql",
        hostname="localhost",
        port=3306,
        username="root",
        password="123456",
        tables=r"app_db\..*",
        server_id="5400-5404",
        server_time_zone="UTC",
    )
    .transform(
        r"app_db.web_orders",
        projection="*, FORMAT('%S', product_name) as product_name",
        filtering="addone(id) > 10 AND order_id > 100",
        primary_keys=["id"],
        description="Transform web orders",
    )
    .route("app_db.orders", "ods_db.ods_orders")
    .route("app_db.shipments", "ods_db.ods_shipments")
    .route("app_db.products", "ods_db.ods_products")
    .with_java_udf("addone", "com.example.functions.AddOneFunctionClass")
    .with_java_udf("format", "com.example.functions.FormatFunctionClass")
    .to_sink(
        "doris",
        fenodes="127.0.0.1:8030",
        benodes="127.0.0.1:8040",
        username="root",
        password="",
    )
)

print("\n" + "=" * 60)
print("Advanced Pipeline YAML:")
print("=" * 60)
print(advanced_pipeline.to_yaml())

# =============================================================================
# Example 5: Pipeline Dictionary
# =============================================================================

print("\n" + "=" * 60)
print("Pipeline Dictionary:")
print("=" * 60)
d = pipeline.to_dict()
for section, content in d.items():
    print(f"{section}: {content}")

# =============================================================================
# Example 6: Execution (commented out to prevent actual execution)
# =============================================================================

# Add connectors (auto-setup happens on first call)
# env.add_connector("flink-cdc-pipeline-connector-mysql")
# env.add_connector("flink-cdc-pipeline-connector-doris")

# Execute pipeline via env - pass pipeline to env
# env.execute(pipeline)

# The execute() method will:
# 1. Validate Flink home exists
# 2. Auto-setup Flink CDC if not already done
# 3. Generate a temporary YAML file
# 4. Run: FLINK_HOME=/path/to/flink flink-cdc.sh pipeline.yaml --jar ...
# 5. All JARs (env.jars + connectors) are passed via --jar

# Or save YAML and use Flink CDC CLI manually:
# pipeline.save_yaml("my-pipeline.yaml")
# Then run:
# FLINK_HOME=/path/to/flink bash .env/flink-cdc-3.3.0/bin/flink-cdc.sh my-pipeline.yaml --jar ...

# =============================================================================
# Example 7: Multiple Pipelines, One Environment
# =============================================================================

# Create multiple pipelines (pure YAML generators)
pipeline_mysql_to_doris = (
    Pipeline.create("MySQL to Doris", parallelism=2)
    .from_source("mysql", hostname="localhost", tables=r"db.\.*")
    .to_sink("doris", fenodes="127.0.0.1:8030")
)

pipeline_pg_to_starrocks = (
    Pipeline.create("PostgreSQL to StarRocks", parallelism=4)
    .from_source("postgres", hostname="localhost", database="mydb")
    .to_sink("starrocks", fenodes="127.0.0.1:9030")
)

# Use one environment to execute different pipelines
# env.execute(pipeline_mysql_to_doris)
# env.execute(pipeline_pg_to_starrocks)

print("\n" + "=" * 60)
print("Multiple pipelines can share one environment:")
print("=" * 60)
print(
    f"MySQL to Doris pipeline: {pipeline_mysql_to_doris.to_dict()['pipeline']['name']}"
)
print(
    f"PG to StarRocks pipeline: {pipeline_pg_to_starrocks.to_dict()['pipeline']['name']}"
)
