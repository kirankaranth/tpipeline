from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def customer_order_details(spark: SparkSession, customers: DataFrame, orders: DataFrame, ) -> DataFrame:
    return customers\
        .alias("customers")\
        .join(orders.alias("orders"), (col("customers.customer_id") == col("orders.customer_id")), "inner")\
        .select(col("customers.customer_id").alias("CUSTOMER_ID"), col("customers.first_name").alias("FIRST_NAME"), col("customers.last_name").alias("LAST_NAME"), col("customers.phone").alias("PHONE"), col("customers.email").alias("EMAIL"), col("customers.country_code").alias("COUNTRY_CODE"), col("customers.account_open_date").alias("ACCOUNT_OPEN_DATE"), col("customers.account_flags").alias("ACCOUNT_FLAGS"), col("orders.order_id").alias("ORDER_ID"), col("orders.order_status").alias("ORDER_STATUS"), col("orders.order_category").alias("ORDER_CATEGORY"), col("orders.order_date").alias("ORDER_DATE"), (col("orders.amount") * lit(82.5)).alias("AMOUNT_IN_RUPEES"))
