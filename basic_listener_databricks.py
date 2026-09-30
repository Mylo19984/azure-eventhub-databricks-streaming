namespace_name = ""
bootstrap_servers = f"{namespace_name}.servicebus.windows.net:9093"
event_hub_name = "" # Acts as the Kafka topic name
 
connection_string = ""
 
sasl_jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" '
    f'password="{connection_string}";'
)
 
# Read stream from Event Hubs via Kafka protocol
streaming_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", event_hub_name)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", sasl_jaas_config)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "30000")
    .option("startingOffsets", "earliest")
    .load()
)
 
# Process the payload
body_df = streaming_df.selectExpr("cast(value as string) as json_payload", "timestamp as enqueuedTime")
 
 
delta_table_path = "abfss://"
checkpoint_path = "abfss://"
 
query = (
    body_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("path", delta_table_path)
    .trigger(availableNow=True)
    .start()
)