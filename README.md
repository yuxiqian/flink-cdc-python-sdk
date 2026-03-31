# Flink CDC Python SDK

[![Python Version](https://img.shields.io/badge/python-≥3.12-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

A Pythonic SDK for developing [Apache Flink CDC](https://github.com/apache/flink-cdc) Pipeline data integration jobs with an elegant fluent API.

## Features

- **Fluent API Design** - Chain methods for intuitive pipeline construction
- **Full Connector Support** - Compatible with all Flink CDC source & sink connectors
- **Transform & Route** - Built-in support for data transformation and routing rules
- **Multiple Data Sources** - Read from/write to CSV, Python objects, or any supported connector
- **UDF Support** - Register and invoke both Java and Python User-Defined Functions
- **AI Integration** - Built-in multi-model AI functions support (OpenAI, etc.)

## Installation

(not available for now)

Requires Python 3.12+.

## Quick Start

```python
from flink_cdc_python import Env, Pipeline

# Create an environment
env = Env.create(
    flink_cdc_version="3.3.0",
    flink_home="/path/to/flink-2.0.0",
)

# Create a simple pipeline and execute
pipeline = (
    Pipeline.create("My First Pipeline", parallelism=2)
    .from_source(
        "mysql",
        hostname="localhost",
        port=3306,
        username="root",
        password="******",
        tables=r"my_db.\.*",
    )
    .to_sink("print")
)

env.execute(pipeline)
```

## Usage Examples

### MySQL to Doris Sync with Transformations

```python
from flink_cdc_python import Env, Pipeline

env = Env.create(
    flink_cdc_version="3.3.0",
    flink_home="/path/to/flink-2.0.0",
)

pipeline = (
    Pipeline.create(
        "Sync MySQL Database to Doris", parallelism=2, schema_change_behavior="evolve"
    )
    .from_source(
        "mysql",
        hostname="localhost",
        port=3306,
        username="root",
        password="123456",
        tables=r"app_db.\.*",
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

env.execute(pipeline)
```

### Using UDFs and AI Models

```python
from flink_cdc_python import Env, Pipeline

env = Env.create(
    flink_cdc_version="3.3.0",
    flink_home="/path/to/flink-2.0.0",
)

pipeline = (
    Pipeline.create("MySQL to Doris with UDF", parallelism=2)
    .from_source(
        "mysql",
        hostname="localhost",
        port=3306,
        username="root",
        password="123456",
        tables=r"app_db.\.*",
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

env.execute(pipeline)
```

### Reading from CSV

```python
from flink_cdc_python import Env, Pipeline

env = Env.create(
    flink_cdc_version="3.3.0",
    flink_home="/path/to/flink-2.0.0",
)

pipeline = (
    Pipeline.create("CSV Pipeline")
    .from_csv("/path/to/input.csv")
    .transform("input", projection="id, name, UPPER(category) as category")
    .to_csv("/path/to/output.csv")
)

env.execute(pipeline)
```

### Collecting Results to Python

```python
from flink_cdc_python import Env, Pipeline

env = Env.create(
    flink_cdc_version="3.3.0",
    flink_home="/path/to/flink-2.0.0",
)

pipeline = (
    Pipeline.create("Collect Pipeline")
    .from_values([(1, "Alice"), (2, "Bob"), (3, "Charlie")])
    .collect()
)

env.execute(pipeline)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Related Projects

- [Apache Flink CDC](https://github.com/apache/flink-cdc) - Flink CDC project
- [Apache Flink](https://flink.apache.org/) - Apache Flink project
