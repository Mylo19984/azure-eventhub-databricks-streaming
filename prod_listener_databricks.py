#from __future__ import annotations  # Python 3.7+ compat for type hints
 
import logging
from typing import Any, Protocol
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, when
from pyspark.sql.types import StructType, StringType, LongType, TimestampType, StructField
from pyspark.sql.streaming import StreamingQueryListener
 
# --- Structured logging (JSON-friendly for log aggregation) ---
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("EventHubsToDeltaPipeline")
 
 
 
# --- Dependency injection via Protocol (testable, no implicit globals) ---
class SecretsClient(Protocol):
    """Minimal protocol for secret retrieval — dbutils or mock."""
    def secrets_get(self, scope: str, key: str) -> str: ...
 
 
class DatabricksSecretsClient:
    """Wraps Databricks dbutils.secrets.get."""
    def __init__(self, dbutils: Any) -> None:
        self._dbutils = dbutils
 
    def secrets_get(self, scope: str, key: str) -> str:
        return self._dbutils.secrets.get(scope=scope, key=key)
 
 
# --- Event schema: enforce contract at ingestion time ---
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),      # Business key
    StructField("event_type", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("payload", StringType(), True),         # Nested payload as string
    StructField("event_time", TimestampType(), True),
])
 
 
def create_kafka_sasl_config(
    namespace_name: str,
    secret_scope: str,
    secret_key: str,
    secrets_client: SecretsClient,
    # temp
    con_string: str,
) -> tuple[str, str]:
    """Build bootstrap servers + JAAS config. Injects secrets client for testability."""
    bootstrap_servers = f"{namespace_name}.servicebus.windows.net:9093"
 
    try:
        #connection_string = secrets_client.secrets_get(scope=secret_scope, key=secret_key)
        connection_string = con_string
    except Exception as e:
        logger.error(f"Secret retrieval failed for scope={secret_scope}, key={secret_key}: {e}")
        raise
 
    # Note: JAAS config may appear in Spark UI — use cluster-scoped conf for max security
    sasl_jaas_config = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" '
        f'password="{connection_string}";'
    )
    return bootstrap_servers, sasl_jaas_config
 
 
 
def build_kafka_read_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    sasl_jaas_config: str,
    event_hub_name: str,
    starting_offsets: str = "latest",          # "earliest" for backfill, "latest" for live
    max_offsets_per_trigger: int = 500_000,    # Throttle to protect cluster memory
    min_offsets_per_trigger: int = 10_000,     # Anti-idle: ensure minimum progress
    request_timeout_ms: int = 60_000,
    session_timeout_ms: int = 30_000,
) -> Any:
    """Configure Kafka source with hardened production defaults."""
    logger.info(f"Building Kafka read stream for topic: {event_hub_name}")
 
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", event_hub_name)
        # --- Auth ---
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", sasl_jaas_config)
        # --- Timeouts (parameterized, not hardcoded) ---
        .option("kafka.request.timeout.ms", str(request_timeout_ms))
        .option("kafka.session.timeout.ms", str(session_timeout_ms))
        # --- Offset management ---
        .option("startingOffsets", starting_offsets)         # Don't silently skip data
        .option("failOnDataLoss", "false")                   # Don't crash if Kafka retention expired
        .option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
        .option("minOffsetsPerTrigger", str(min_offsets_per_trigger))
        # --- Identifiability in Kafka metrics ---
        .option("kafka.client.id", f"spark-eh-{event_hub_name}-consumer")
        .load()
    )
 
 
 
def parse_and_validate(streaming_df: Any) -> Any:
    """Parse JSON payload, validate against schema, flag invalid records for DLQ."""
    return (
        streaming_df
        .selectExpr(
            "CAST(value AS STRING) as raw_payload",
            "timestamp as kafka_enqueued_time",
            "partition",
            "offset",
        )
        # Handle null payloads (tombstone / empty Kafka messages)
        .withColumn("raw_payload", when(col("raw_payload").isNull(), lit("{}")).otherwise(col("raw_payload")))
        # Schema-aware parsing — enables predicate pushdown, type safety, queryability
        .withColumn("parsed_event", from_json(col("raw_payload"), EVENT_SCHEMA))
        .withColumn("is_valid", col("parsed_event").isNotNull())
        .withColumn("pipeline_processed_time", current_timestamp())
    )
 
 
 
def create_foreach_batch_writer(
    delta_table_path: str,
    dlq_path: str,
    event_hub_name: str,
) -> Any:
    """Returns a foreachBatch handler: valid → Delta (upsert), invalid → DLQ."""
 
    def write_batch(batch_df: Any, batch_id: int) -> None:
        valid_df = batch_df.filter(col("is_valid")).drop("raw_payload", "is_valid")
        invalid_df = batch_df.filter(~col("is_valid"))
 
  # .count() can create overhead for speed if needed.
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
 
        if valid_count > 0:
            # Idempotent append — txnAppId prevents duplicates on retry
            (
                valid_df.write
                .format("delta")
                .mode("append")
                .option("txnAppId", f"eh-{event_hub_name}-stream")
                .option("txnVersion", batch_id)
                .option("mergeSchema", "true")    # Allow additive schema evolution
                .save(delta_table_path)
            )
 
        # Dead Letter Queue — poison pills don't crash the pipeline
        if invalid_count > 0:
            (
                invalid_df.write
                .format("delta")
                .mode("append")
                .save(dlq_path)
            )
            logger.warning(f"Batch {batch_id}: {invalid_count} invalid records → DLQ")
 
        logger.info(f"Batch {batch_id}: {valid_count} valid rows → Delta | {invalid_count} invalid → DLQ")
 
    return write_batch


class StreamingQListener(StreamingQueryListener):
    """Spark streaming query listener for lag detection and alerting."""
 
    def onQueryStarted(self, event: Any) -> None:
        logger.info(f"Query started: {event.id}")
 
    def onQueryProgress(self, event: Any) -> None:
        p = event.progress
        input_rate = p.inputRowsPerSecond
        proc_rate = p.processedRowsPerSecond
 
        logger.info(
            f"Batch {p.batchId}: input={input_rate:.1f}/s, "
            f"processed={proc_rate:.1f}/s, rows={p.numInputRows}"
        )
 
        # Alert on consumer lag buildup
        if input_rate > 0 and proc_rate > 0 and input_rate > proc_rate * 1.5:
            logger.warning(
                f"Consumer lag detected! input={input_rate:.1f}/s > "
                f"processing={proc_rate:.1f}/s — scale up or reduce batch size"
            )
 
    def onQueryTerminated(self, event: Any) -> None:
        logger.info(f"Query terminated: {event.id}, exception={event.exception}")
 
 
def start_event_hub_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    sasl_jaas_config: str,
    event_hub_name: str,
    delta_table_path: str,
    checkpoint_path: str,
    dlq_path: str,
    trigger_mode: str = "continuous",        # "continuous" or "available_now"
    trigger_interval_seconds: int = 30,       # Interval for continuous mode
) -> None:
    """Orchestrate the full streaming pipeline: read → parse → route → write."""
 
    logger.info(f"Starting pipeline for Event Hub: {event_hub_name}")
 
    # 1. Read
    streaming_df = build_kafka_read_stream(spark, bootstrap_servers, sasl_jaas_config, event_hub_name)
 
    # 2. Parse & validate
    enriched_df = parse_and_validate(streaming_df)
 
    # 3. Write via foreachBatch (multi-sink: Delta + DLQ)
    batch_writer = create_foreach_batch_writer(delta_table_path, dlq_path, event_hub_name)
 
    # 4. Configure trigger based on intent
    if trigger_mode == "available_now":
        # Batch-like: process available offsets then self-terminate (for scheduled runs)
        trigger = dict(availableNow=True)
    else:
        # Continuous: micro-batches at fixed interval (for always-on streaming)
        trigger = dict(processingTime=f"{trigger_interval_seconds} seconds")
 
    query = (
        enriched_df.writeStream
        .foreachBatch(batch_writer)
        .outputMode("append")
        .trigger(**trigger)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    Spark.streams.addListener(StreamingQListener())
 
    logger.info(f"Stream started with trigger={trigger_mode}. Awaiting termination...")
    query.awaitTermination()
    logger.info("Stream terminated.")

 
# --- Execution Entrypoint ---
if __name__ == "__main__":
    # --- Config (in production, read from env vars / cluster config) ---
    NAMESPACE = ""
    TOPIC_NAME = ""
    SECRET_SCOPE = "your-databricks-secret-scope"
    SECRET_KEY = "your-eventhub-connection-string-key"
    CON_S = ''
 
    DELTA_PATH = "abfss://"
    CHECKPOINT_PATH = "abfss://"
    DLQ_PATH = "abfss://"
 
    TRIGGER_MODE = "available_now"          # "continuous" for always-on, "available_now" for scheduled
    STARTING_OFFSETS = "earliest"          # "earliest" on first run to backfill
 
 
secrets_client = DatabricksSecretsClient(dbutils)
 
servers, jaas_config = create_kafka_sasl_config(
        namespace_name=NAMESPACE,
        secret_scope=SECRET_SCOPE,
        secret_key=SECRET_KEY,
        secrets_client=secrets_client,
        con_string=CON_S,
    )
 
 
start_event_hub_stream(
        spark=spark,
        bootstrap_servers=servers,
        sasl_jaas_config=jaas_config,
        event_hub_name=TOPIC_NAME,
        delta_table_path=DELTA_PATH,
        checkpoint_path=CHECKPOINT_PATH,
        dlq_path=DLQ_PATH,
        trigger_mode=TRIGGER_MODE,
    )