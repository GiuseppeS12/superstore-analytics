from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff,to_date,udf
from pyspark.sql.functions import sha2, concat,lower,trim,cast
from pyspark.sql.types import StringType, IntegerType
import os

DATASET   = "superstore_dataset2011-2015.csv"
DATA_PATH = "your_directory/superstores-analytics/data"

def augment_data():
    spark = SparkSession.builder \
        .appName("augment_superstore") \
        .getOrCreate()

    superstore = spark.read.format('csv') \
        .option('header', True) \
        .option('inferSchema', True) \
        .load(f"{DATA_PATH}/{DATASET}")

    columns = superstore.columns
    renamed_columns = tuple(map(lambda colname: colname.lower().replace(' ', '_').replace('-', '_'), columns))

    superstore = superstore.withColumnsRenamed(dict(zip(columns, renamed_columns)))

    superstore = superstore.filter(~superstore["sales"].rlike("[a-zA-Z]"))

    hash_assembly = 'city','state','country','region','market'

    superstore = superstore.withColumn('region_id',sha2(concat(*(lower(trim(col(c))) for c in hash_assembly)),256))
    superstore.coalesce(1).write.format('csv').option('header',True).option('inferSchema',True).mode('overwrite').save(f"{DATA_PATH}/superstore")

    spark.stop()


def transform_data():
    spark = SparkSession.builder \
        .appName("superstore_transform") \
        .getOrCreate()

    superstore = spark.read.format('csv') \
        .option('header', True) \
        .option('inferSchema', True) \
        .load(f"{DATA_PATH}/superstore.csv")

    transformation = udf(lambda s: s.replace('/','-'), StringType())

    # superstore.columns
    columns = 'Row ID', 'Order ID', 'Customer ID', 'Product ID','Region ID','Order Date', 'Ship Date', 'Sales', 'Quantity', 'Discount', 'Profit', 'Shipping Cost','Order Priority'
    renamed_columns = tuple(map(lambda colname: colname.lower().replace(' ','_'),columns))
    fact_table = superstore.select(*renamed_columns)

    fact_table = fact_table.withColumn('row_id',col('row_id').cast('int'))
    fact_table = fact_table.withColumn('sales',col('sales').cast('double'))
    fact_table = fact_table.withColumn('quantity',col('quantity').cast('double'))
    fact_table = fact_table.withColumn('discount',col('discount').cast('double'))
    fact_table = fact_table.withColumn('profit',col('profit').cast('double'))
    fact_table = fact_table.withColumn('shipping_cost',col('shipping_cost').cast('double'))


    fact_table = fact_table.withColumn('order_date',transformation(col('order_date')))
    fact_table = fact_table.withColumn('ship_date',transformation(col('ship_date')))

    fact_table = fact_table.withColumn("order_date", to_date("order_date", "d-M-yyyy"))
    fact_table = fact_table.withColumn("ship_date", to_date("ship_date", "d-M-yyyy"))
    
    fact_table.write.mode('overwrite').parquet(f"{DATA_PATH}/fact_table")

    fact_table.printSchema()

    spark.stop()

def build_data_mart():
    spark = SparkSession.builder \
        .appName("superstore_datamart") \
        .getOrCreate()

    superstore = spark.read.format('csv') \
        .option('header', True) \
        .option('inferSchema', True) \
        .load(f"{DATA_PATH}/superstore.csv")

    dim_customer = superstore.select('customer_id','customer_name','segment')
    dim_product  = superstore.select('product_id','product_name','category','sub_category')
    dim_region   = superstore.select('region_id','city','state','country','market','region','postal_code')

    dim_customer.write.mode('overwrite').parquet(f"{DATA_PATH}/customer_data")
    dim_product.write.mode('overwrite').parquet(f"{DATA_PATH}/product_data")
    dim_region.write.mode('overwrite').parquet(f"{DATA_PATH}/region_data")

    spark.stop()

def upload_to_db():
    spark = SparkSession.builder \
        .appName("superstore_on_db") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .getOrCreate()

    for parquet in os.listdir(DATA_PATH):
        if os.path.isdir(f"{DATA_PATH}/{parquet}"):
            info = spark.read.format('parquet').load(f"{DATA_PATH}/{parquet}")

            info.write \
                .format("jdbc") \
                .option("url", "jdbc:mysql://localhost:3306/superstore") \
                .option("dbtable", f"{parquet}") \
                .option("user", "user") \
                .option("password", "12345") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("overwrite") \
                .save()

    spark.stop()
