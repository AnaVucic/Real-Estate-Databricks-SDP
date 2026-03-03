from pyspark import pipelines as dp
from pyspark.sql.functions import col, date_format, year, month, quarter, dayofmonth, dayofweek, concat, lit, weekofyear, dayofyear, when, current_timestamp

start_date = spark.conf.get('start_date')
end_date = spark.conf.get('end_date')

@dp.materialized_view(
    name='real_estate.silver.calendar',
    comment='Calendar dimension with date attributes',
    table_properties={
        'quality': 'silver',
        'layer': 'silver',
        'delta.enableChangeDataFeed': 'true',
        'delta.autoOptimize.optimizeWrite': 'true',
        'delta.autoOptimize.autoCompact': 'true'
    }
)
def calendar():
    df = spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'), 
                to_date('{end_date}'), 
                interval 1 day)) as date
        """
    )

    df = df.withColumn(
        'date_key', date_format(col('date'), 'yyyyMMdd').cast('int')    
    )

    df = (
        df.withColumn('year', year(col('date')))
        .withColumn('month', month(col('date')))
        .withColumn('quarter', quarter(col('date')))
    )

    df = (
        df.withColumn('day_of_month', dayofmonth(col('date')))
        .withColumn('day_of_week', date_format(col('date'), 'EEEE'))
        .withColumn('day_of_week_abbr', date_format(col('date'), 'EEE'))
        .withColumn('day_of_week_num', dayofweek(col('date')))
    )

    df = (
        df.withColumn('month_name', date_format(col('date'), 'MMMM'))
        .withColumn('month_year', concat(
            date_format(col('date'), 'MMMM'),
            lit(' '),
            col('year')
            ))
        .withColumn('quarter_year', concat(
            lit('Q'),
            col('quarter'),
            lit(' '),
            col('year')))
    )

    df = df.withColumn(
        'week_of_year', weekofyear(col('date'))
    ).withColumn(
        'day_of_year', dayofyear(col('date')) 
    )

    df = df.withColumn(
        'is_weekend',
        when(col('day_of_week_num').isin([1, 7]), True).otherwise(False)
    ).withColumn(
        'is_weekday',
        when(col('day_of_week_num').isin([1,7]), False).otherwise(True)
    )   

    df = df.withColumn(
        'silver_processed_timestamp', current_timestamp()
    )

    df = df.select(
         'date',
         'date_key',
         'year',
         'month',
         'day_of_month',
         'day_of_week',
         'day_of_week_abbr',
         'day_of_week_num',
         'month_name',
         'month_year',
         'quarter',
         'quarter_year',
         'week_of_year',
         'day_of_year',
         'is_weekday',
         'is_weekend',
         'silver_processed_timestamp'
    )

    return df