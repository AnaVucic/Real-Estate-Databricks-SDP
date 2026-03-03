from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, ArrayType, StringType
from pyspark.pandas import *

def slugify(col):
    c = F.trim(col)
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.lower(c)
    c = F.translate(c, "čćšž", "ccsz")
    c = F.regexp_replace(c, r"đ", "dj")
    c = F.regexp_replace(c, r"[^a-z0-9]+", "_")
    return c

@dp.view(
    name='postings_silver_staging',
    comment='Transformed postings data ready for CDC upsert.'
)
@dp.expect('valid_price','price_amount >= 0') 
def postings_silver():
    df_bronze = spark.readStream.table('real_estate.bronze.postings')
    
    df_silver = df_bronze.select(
        F.col('PostingId').alias('id'),
        F.col('Title').alias('title'),
        F.col('City').alias('city'),
        F.col('Location').alias('location'),
        F.col('Microlocation').alias('microlocation'),
        F.col('PostingDate').alias('posting_date'),
        F.col('Price').alias('price'),
        F.col('Street').alias('street'),
        F.col('Type').alias('type'),
        F.col('Rooms').alias('rooms'),
        F.col('Area').alias('area'),
        F.col('Poster').alias('poster'),
        F.col('Heating').alias('heating'),
        F.col('Furnished').alias('furnished'),
        F.col('Floor').alias('floor'),
        F.col('FloorTotal').alias('floor_total'),
        F.col('PaymentType').alias('payment_type'),
        F.col('Additional').alias('additional'),
        F.col('Other').alias('other'),
        F.col('Description').alias('description'),
        F.col('ingest_datetime').alias('bronze_ingest_datetime')
    )

    df_silver = df_silver.withColumn(
        'price_amount',
        F.regexp_replace(F.split(F.col('price'), ' ')[0], r"\.", '').cast(IntegerType())
    ).withColumn(
        'currency',
        F.split(F.col('price'), ' ')[1]
    )

    df_silver = df_silver.withColumn(
        'area_amount',
        F.regexp_replace(F.split(F.col('area'), ' ')[0], r",", '.').cast(DoubleType())
    ).withColumn(
        'measurement_unit',
        F.split(F.col('area'), ' ')[1]
    )

    df_silver = df_silver.withColumn(
        'posting_datetime',
        F.to_timestamp(
            F.regexp_replace(F.col('posting_date'), r' u ', ' '),
            format='dd.MM.yyyy. HH:mm'
        )
    )

    df_silver = df_silver.withColumn(
        'silver_processed_timestamp', F.current_timestamp()
    )

    df_silver = df_silver.drop('area').drop('price').drop('posting_date').drop('other').drop('additional')

    return df_silver


# DIM TABLES
@dp.table(
    name='silver.dim_additional_tags',
    comment='Materialized view for dimension `additional` tags.'
)
def dim_additional_tags():
    df = spark.read.table('real_estate.bronze.postings').select('PostingId', 'Additional')
    df = df.withColumn('silver_processed_timestamp', F.current_timestamp())
    df = df.select(
        F.explode(
            F.from_json(F.col('additional'), ArrayType(StringType()))
        ).alias('additional_tag_name')
    ).distinct()

    df = df.withColumn(
        'additional_tag_slug',
        slugify(F.col('additional_tag_name'))
    ).withColumn(
        'additional_tag_id',
        F.sha2(F.col('additional_tag_slug'), 256)
    )

    return df
@dp.table(
    name='silver.dim_other_tags',
    comment='Materialized view for dimension `other` tags.'
)
def dim_other_tags():
    df = spark.read.table('real_estate.bronze.postings').select('PostingId', 'Other')
    df = df.withColumn('silver_processed_timestamp', F.current_timestamp())
    df = df.select(
        F.explode(
            F.from_json(F.col('other'), ArrayType(StringType()))
        ).alias('other_tag_name')
    ).distinct()

    df = df.withColumn(
        'other_tag_slug',
        slugify(F.col('other_tag_name'))
    ).withColumn(
        'other_tag_id',
        F.sha2(F.col('other_tag_slug'), 256)
    )
    return df

# BRIDGE TABLES
@dp.table(
    name='silver.postings_additional_tags',
    comment='Many-to-many table connecting postings and `additional` tags.'
)
def postings_additional_tags():
    postings = spark.readStream.table('real_estate.bronze.postings')
    tags = spark.read.table('silver.dim_additional_tags')

    # Explode additional column to get tags per posting
    exploded = postings.select(
        F.col('PostingId').alias('posting_id'),
        F.explode(
            F.from_json(F.col('additional'), ArrayType(StringType()))
        ).alias('additional_tag_name')
    )

    # Generate slug and id for tag
    exploded = exploded.withColumn(
        'additional_tag_slug',
        slugify(F.col('additional_tag_name'))
    ).withColumn(
        'additional_tag_id',
        F.sha2(F.col('additional_tag_slug'), 256)
    )

    # Join with dim table to ensure tag exists, add new tags if missing
    # (Lakeflow Declarative Pipelines will handle schema evolution and upserts)
    return exploded.select('posting_id', 'additional_tag_id', 'additional_tag_name')

@dp.table(
    name='silver.postings_other_tags',
    comment='Many-to-many table connecting postings and `other` tags.'
)
def postings_other_tags():
    postings = spark.readStream.table('real_estate.bronze.postings')
    tags = spark.read.table('silver.dim_other_tags')

    # Explode other column to get tags per posting
    exploded = postings.select(
        F.col('PostingId').alias('posting_id'),
        F.explode(
            F.from_json(F.col('other'), ArrayType(StringType()))
        ).alias('other_tag_name')
    )

    # Generate slug and id for tag
    exploded = exploded.withColumn(
        'other_tag_slug',
        slugify(F.col('other_tag_name'))
    ).withColumn(
        'other_tag_id',
        F.sha2(F.col('other_tag_slug'), 256)
    )

    # Join with dim table to ensure tag exists, add new tags if missing
    # (Lakeflow Declarative Pipelines will handle schema evolution and upserts)
    return exploded.select('posting_id', 'other_tag_id', 'other_tag_name')

# POSTINGS
dp.create_streaming_table(
    name='real_estate.silver.postings',
    comment='Cleaned and validated orders with CDC upsert capability.',
    table_properties={
        'quality': 'silver',
        'layer':'silver',
        'delta.enableChangeDataFeed': 'true',
        'delta.autoOptimize.OptimizeWrite':'true',
        'delta.autoOptimize.autoCompact':'true'
        },
)

dp.create_auto_cdc_flow(
    target='real_estate.silver.postings',
    source='postings_silver_staging',
    keys=['id'],
    sequence_by=F.col('silver_processed_timestamp'),
    stored_as_scd_type=1,
    except_column_list=[]
)