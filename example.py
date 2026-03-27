from flink_cdc_python import Pipeline

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
        tables="app_db.\.*",
    )
    .transform(
        "app_db.orders",
        projection="id, order_id, UPPER(product_name) as product_name",
        filtering="id > 10 AND order_id > 100",
    )
    .route("app_db.orders", "ods_db.ods_orders")
    .route("app_db.shipments", "ods_db.ods_shipments")
    .route("app_db.\.*", "ods_db.others")
    .to_sink("doris", fenodes="127.0.0.1:8030", username="root", password="123456")
)


def format_udf(fmt, *args) -> str:
    return fmt % args


pipeline.with_java_udf(
    "addone", "com.example.functions.AddOneFunctionClass"
).with_python_udf("format", format_udf).with_ai_model(
    "CHAT",
    "OpenAIChatModel",
    openai_model="chat-3-small",
    openai_host="https://example.com/openai/",
    openai_apikey="XXX",
    openai_chat_prompt="Please summary this",
).with_ai_model(
    "GET_EMBEDDING",
    "OpenAIEmbeddingModel",
    openai_model="text-embedding-3-small",
    openai_host="https://example.com/openai/",
    openai_apikey="XXX",
).execute()
