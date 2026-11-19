"""ETL local del taller: calidad, joins, agregación e incremental idempotente."""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("taller-data-engineer").getOrCreate()
base = "datos"

def csv(name):
    return spark.read.option("header", True).option("inferSchema", False).csv(f"{base}/{name}.csv")

customers = csv("customers")
products = csv("products")
orders = csv("orders")
items = csv("order_items")

def typed_orders(df):
    # try_cast convierte fechas mal formadas a NULL para poder cuantificarlas
    # como anomalías sin abortar todo el job.
    return (df.withColumn("order_ts", F.expr("try_cast(order_ts AS timestamp)"))
        .withColumn("updated_at", F.expr("try_cast(updated_at AS timestamp)")))

def typed_products(df):
    return df.withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))

def typed_items(df):
    return (df.withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("discount_pct", F.col("discount_pct").cast("decimal(8,4)")))

customers = customers.dropDuplicates(["customer_id"])
products = typed_products(products).dropDuplicates(["product_id"])
orders = typed_orders(orders).dropDuplicates(["order_id"])
items = typed_items(items).dropDuplicates(["line_id"])

# Rechazo lógico: solo líneas con claves, cantidad/precio válidos y descuento entre 0 y 1.
valid_items = (items.join(orders.select("order_id"), "order_id", "left_semi")
    .join(products.select("product_id", "unit_price"), "product_id", "inner")
    .where((F.col("quantity") > 0) & F.col("discount_pct").between(0, 1))
    .withColumn("net_amount", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct")), 2)))

detail = (valid_items.join(orders, "order_id", "inner")
    .join(customers.select("customer_id", "country"), "customer_id", "inner")
    .join(products.select("product_id", "category"), "product_id", "inner")
    .withColumn("month", F.date_format("order_ts", "yyyy-MM")))

summary = (detail.groupBy("month", "country", "channel", "category")
    .agg(F.round(F.sum("net_amount"), 2).alias("net_sales"),
         F.countDistinct("order_id").alias("orders"),
         F.count("line_id").alias("lines")))

summary.write.mode("overwrite").partitionBy("month").parquet("output/monthly_sales")
detail.write.mode("overwrite").parquet("output/valid_detail")

print("QA líneas válidas:", valid_items.count())
print("QA total detalle:", detail.agg(F.round(F.sum("net_amount"), 2).alias("total")).first()["total"])
print("QA total resumen:", summary.agg(F.round(F.sum("net_sales"), 2).alias("total")).first()["total"])

# Incremental idempotente: unión, versión más reciente por clave y reejecución segura.
inc_orders = spark.read.json(f"{base}/incremental/orders_incremental.jsonl")
inc_items = spark.read.json(f"{base}/incremental/order_items_incremental.jsonl")
all_orders = orders.unionByName(typed_orders(inc_orders), allowMissingColumns=True)
latest = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc_nulls_last())
all_orders = all_orders.withColumn("rn", F.row_number().over(latest)).where("rn = 1").drop("rn")
all_orders.write.mode("overwrite").parquet("output/orders_idempotent")

spark.stop()
