"""Spark normalization and monthly event aggregation."""
import json
import shutil
from pathlib import Path
from pipeline.core import safe_id, write_json


def transform(config, data_dir, run_id):
    from pyspark.sql import SparkSession, functions as F, types as T
    root = Path(data_dir).resolve()
    raw = root / 'raw' / safe_id(run_id)
    version = root / 'versions' / run_id
    if (version / 'manifest.json').exists():
        return version
    staging = version.with_name(run_id + '.partial')
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    spark = SparkSession.builder.master('local[2]').appName('city-permits').config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate()
    try:
        schema = T.StructType([T.StructField(k, T.StringType()) for k in set(config['fields'].values()) | {config['building_type_field']}])
        source = spark.read.schema(schema).json(str(raw / 'permits.jsonl'))
        fields = config['fields']
        def col(name):
            return F.col(fields[name]) if name in fields else F.lit(None).cast('string')
        def number(name):
            return col(name).cast('double')
        def date(name):
            return F.to_date(F.substring(col(name),1,10))
        classification = F.col(config['building_type_field'])
        residential = [v.lower() for v in config['residential_values']]
        normalized = source.select(
            F.lit(config['jurisdiction']).alias('jurisdiction'), F.lit(config['source']).alias('source'),
            col('permit_id').alias('permit_id'), col('status').alias('status'),
            col('permit_type').alias('permit_type'), col('permit_subtype').alias('permit_subtype'),
            col('address').alias('address'), number('latitude').alias('latitude'), number('longitude').alias('longitude'),
            classification.alias('source_building_type'),
            F.when(F.lower(classification).isin(residential), 'residential').when(classification.isNull(), 'unknown').otherwise('nonresidential').alias('building_type'),
            date('applied_date').alias('applied_date'), date('issued_date').alias('issued_date'), date('completed_date').alias('completed_date'),
            number('units_added').alias('units_added'), number('units_removed').alias('units_removed'), number('net_units').alias('source_net_units'), number('estimated_cost').alias('estimated_cost'))
        statuses = config.get('stage_values', {})
        status = F.lower(F.col('status'))
        normalized = normalized.withColumn('net_units', F.coalesce(F.col('source_net_units'), F.col('units_added')-F.col('units_removed'))).withColumn('stage',
            F.when(status.isin(statuses.get('cancelled',['cancelled','canceled','withdrawn'])), 'cancelled')
            .when(status.isin(statuses.get('expired',['expired'])), 'expired')
            .when(F.col('completed_date').isNotNull() | status.isin(statuses.get('completed',['completed','finaled','final'])), 'completed')
            .when(status.isin(['denied','closed']), 'closed')
            .when(F.col('issued_date').isNotNull(), 'issued').otherwise('application'))
        # Include any documented event since 2010; retain undated rows for coverage.
        dates = ['applied_date','issued_date','completed_date']
        normalized = normalized.filter((F.greatest(*[F.col(c) for c in dates]) >= F.lit('2010-01-01').cast('date')) | F.greatest(*[F.col(c) for c in dates]).isNull())
        count = normalized.count()
        if not count:
            raise ValueError('No records with documented activity since 2010')
        normalized.write.mode('overwrite').parquet(str(staging / 'permits'))
        events = None
        for event, field in [('applied','applied_date'), ('issued','issued_date'), ('completed','completed_date')]:
            part = normalized.filter(F.col(field) >= F.lit('2010-01-01').cast('date')).select('*',F.trunc(field,'month').alias('month'),F.lit(event).alias('event'))
            events = part if events is None else events.unionByName(part)
        monthly = events.groupBy('jurisdiction','source','month','event','building_type','permit_type').agg(F.count('*').alias('permit_count'), F.sum('units_added').alias('units_added'),F.sum('units_removed').alias('units_removed'),F.sum('net_units').alias('net_units'), F.sum(F.when(F.col('units_added').isNull() | F.col('units_removed').isNull(),1).otherwise(0)).alias('unknown_units'))
        monthly.write.mode('overwrite').parquet(str(staging / 'monthly'))
        metadata = json.loads((raw / 'metadata.json').read_text())
        write_json(staging / 'manifest.json', {**metadata, 'normalized_row_count':count, 'schema_version':1, 'undated_records':normalized.filter(F.greatest(*[F.col(c) for c in dates]).isNull()).count(), 'missing_completed_dates':normalized.filter((F.col('stage')=='completed') & F.col('completed_date').isNull()).count()})
        version.parent.mkdir(parents=True, exist_ok=True)
        staging.rename(version)
        return version
    finally:
        spark.stop()
