from pyspark import pipelines as dp
import pyspark.sql.functions as F

SOURCE_PATH = 's3://halo-oglasi-beograd-izdavanje-bucket/postings/'

@dp.table(
    name='real_estate.bronze.postings',
    comment='Streaming ingestion of daily raw postings data from Halo Oglasi Website with Auto Loader.',
    table_properties={
        'quality':'bronze',
        'layer': 'bronze',
        'source_format':'csv',
        'delta.enableChangeDataFeed':'true',
        'delta.autoOptimize.optimizeWrite':'true',
        'delta.autoOptimize.autoCompact':'true',
    }
)
def postings_bronze():
    df = (
        spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format', 'csv')
        .option('delimiter', '\t')
        .option('cloudFiles.inferColumnTypes', 'true')
        .option('cloudFiles.schemaEvolutionMode', 'rescue')
        .option('cloudFiles.maxFilesPerTrigger', 50)
        .load(SOURCE_PATH)
    )

    df = df.withColumn('file_name', F.col('_metadata.file_path')) \
        .withColumn('ingest_datetime', F.current_timestamp())
    
    return df
