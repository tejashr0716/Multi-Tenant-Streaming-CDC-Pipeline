"""
StreamForge Apache Spark Streaming & Lakehouse CDC Compactor
Executes Upsert/Merge compaction across raw CDC mutations to produce deduplicated latest state.
"""

import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)

def build_spark_session(app_name: str = "StreamForge-CDC-Compactor") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

def run_cdc_compaction(raw_events_json_path: str, output_report_path: str):
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("event_id", LongType(), True),
        StructField("tenant_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("op_type", StringType(), True),
        StructField("record_pk", StringType(), True),
        StructField("payload_json", StringType(), True),
        StructField("lsn_offset", LongType(), True),
        StructField("partition_id", IntegerType(), True)
    ])

    df = spark.read.schema(schema).json(raw_events_json_path)

    # 1. Delta Lake / Lakehouse MERGE Logic: Deduplicate to latest LSN per primary key
    window_spec = Window.partitionBy("tenant_id", "table_name", "record_pk").orderBy(F.col("lsn_offset").desc())
    df_ranked = df.withColumn("row_rank", F.row_number().over(window_spec))

    latest_records = df_ranked.filter(F.col("row_rank") == 1)

    # 2. Exclude Deleted Records (Tombstone Purge)
    active_records = latest_records.filter(F.col("op_type") != "DELETE")

    # Aggregations
    total_mutations = df.count()
    unique_keys = latest_records.count()
    active_keys = active_records.count()
    deleted_keys = unique_keys - active_keys

    # Partition Distribution
    part_stats = df.groupBy("partition_id").count().orderBy("partition_id")
    partition_distribution = {str(row["partition_id"]): row["count"] for row in part_stats.collect()}

    # Tenant Breakdown
    tenant_stats = df.groupBy("tenant_id").agg(
        F.count("event_id").alias("total_mutations"),
        F.countDistinct("record_pk").alias("distinct_entities")
    )
    tenant_breakdown = [row.asDict() for row in tenant_stats.collect()]

    report = {
        "engine": "Apache Spark 4.2 Streaming CDC Compactor",
        "total_raw_mutations": total_mutations,
        "compacted_active_records": active_keys,
        "purged_deleted_records": deleted_keys,
        "compaction_efficiency_pct": round(((total_mutations - active_keys) / total_mutations * 100.0) if total_mutations > 0 else 0.0, 2),
        "partition_distribution": partition_distribution,
        "tenant_breakdown": tenant_breakdown
    }

    os.makedirs(os.path.dirname(output_report_path), exist_ok=True)
    with open(output_report_path, "w") as f:
        json.dump(report, f, indent=2)

    spark.stop()
    return report

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark_cdc_compactor.py <input_json> <output_json>")
        sys.exit(1)
    run_cdc_compaction(sys.argv[1], sys.argv[2])
