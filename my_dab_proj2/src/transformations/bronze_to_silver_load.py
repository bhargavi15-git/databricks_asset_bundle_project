from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from word2number import w2n

def word_to_num(value):
    try:
        # If already numeric
        return int(value)
    except:
        try:
            return w2n.word_to_num(value.lower())
        except:
            return None
word_to_num_udf = udf(word_to_num, IntegerType())

def bonus_calculator(role:str,age:int):
    if role.upper()=="DRIVER" and age>50:
        return 0.15
    elif role.upper()=="DRIVER" and age<30:
        return 0.05
    else:
        return 0
bonus_udf = udf(bonus_calculator)

def mask_string(name:str):
    if name is None:
        return None
    if len(name) <= 2:
        return name
    return name[:2] + "*" * (len(name) - 3) + name[-1]
mask_string_udf=udf(mask_string)


@dp.view(name="cleansed_staff_vw")
@dp.expect_all_or_drop({"shipment_id_is_not_null":"shipment_id IS NOT NULL","role_is_not_null":"role is NOT NULL","name_not_null": "first_name is not null or last_name is not null "})


def cleansed_staff():
    df1=spark.read.table("etl_pipeline_catalog.etl_pipeline_schema.bronze_staff1_table")
    df2=spark.read.table("etl_pipeline_catalog.etl_pipeline_schema.bronze_staff2_table")
    staff_df=df1.unionByName(df2, allowMissingColumns=True)
    return staff_df.orderBy(col("shipment_id").asc(),col("source_cat").desc()).distinct().dropDuplicates(["shipment_id"]).na.drop(how="all",subset=['first_name','last_name'])


@dp.view(name="scrubbed_staff_vw")
def srubbed_staff():
    df3=spark.read.table("cleansed_staff_vw")
    return df3.na.fill(-1,['age']).na.fill('UNKNOWN',['vehicle_type']).na.replace({"ten":"-1","": "-1"},subset=['age']).na.replace({"Truck":"LMV","Bike":"TwoWheeler"},subset=['vehicle_type'])

@dp.view(name="standard_staff_vw")
def staff_data_standardisation_func():
    df4=spark.read.table("scrubbed_staff_vw")
    return (
        df4.withColumn("role",lower(col("role")))
        .withColumn("hub_location",initcap(col("hub_location")))
        .withColumn("age",word_to_num_udf(col("age")).cast("int"))
        .withColumn("shipment_id",word_to_num_udf(col("shipment_id"))
        .cast("int"))
        .withColumnRenamed("first_name","staff_first_name")
        .withColumnRenamed("last_name","staff_last_name")
        .withColumnRenamed("hub_location","origin_hub_city"))
    
@dp.view(name="enriched_staff_vw")
def enriched_staff():
    df5=spark.read.table("standard_staff_vw")
    return (
        df5.withColumn("load_dt",lit(current_timestamp()))
        .withColumn("full_name",concat(col("staff_first_name"),lit(" "),col("staff_last_name")))
        .select('shipment_id','full_name', 'age', 'role', 'origin_hub_city', 'vehicle_type', 'load_dt','source_cat')).withColumn("projected_bonus",bonus_udf(col("role"),col("age"))).withColumn("full_name",mask_string_udf(col("full_name")))

@dp.table(name="silver_staff_table",comment="Silver staff table",table_properties={"quality":"silver"})
def silver_staff_table():
    return spark.read.table("enriched_staff_vw")

@dp.view(name="standard_shipment_vw")
def logistics_shipment_data_standarisation_func():
    df6=spark.read.table("etl_pipeline_catalog.etl_pipeline_schema.bronze_shipments_table")
    return (
        df6.withColumn("domain",lit("Logistics"))
        .withColumn("is_expedited",lit(False))
        .withColumn("ingestion_timestamp",lit(current_timestamp()))
        .withColumn("vehicle_type",upper(col("vehicle_type")))
        .withColumn("shipment_date",to_date("shipment_date","yy-MM-dd"))
        .withColumn("shipment_cost",round(col("shipment_cost"),2))
        .withColumn("shipment_weight_kg",col("shipment_weight_kg").cast("double"))
        .withColumn("is_expedited",col("is_expedited").cast("boolean")))
    
@dp.view(name="enriched_shipment_vw")
def logistics_shipment_data_enrichment_func():
    df7=spark.read.table("standard_shipment_vw")
    return (
        df7.withColumn("route_segment",concat(col("source_city"),lit("-"),col("destination_city")))
        .withColumn("vehicle_identifier",concat(col("vehicle_type"),lit("_"),col("shipment_id")))
        .withColumn("shipment_year",year(col("shipment_date")))
        .withColumn("shipment_month",month(col("shipment_date")))
        .withColumn("is_weekend",when((dayofweek(col("shipment_date")) == 1) | (dayofweek(col("shipment_date")) == 7 ),True).otherwise(False))
        .withColumn("is_expedited",when((col("shipment_status") == 'IN_TRANSIT') | (col("shipment_status") == 'DELIVERED'), True).otherwise(False))
        .withColumn("cost_per_kg", try_divide(col("shipment_cost"), col("shipment_weight_kg")))
        .withColumn("days_since_shipment",datediff(current_date(),col("shipment_date")))
        .withColumn("tax_amount",col("shipment_cost")*0.18)
        .withColumn("order_prefix", substring(col("order_id"), 1, 3))
        .withColumn("order_sequence", substring(col("order_id"), 4, length(col("order_id"))))
        .withColumn("ship_year",year(col("shipment_date")))
        .withColumn("ship_month",month(col("shipment_date")))
        .withColumn("ship_day",day(col("shipment_date")))
        .withColumn("route_lane",concat(col("source_city"),lit("->"),col("destination_city"))))

@dp.table(name="silver_shipment_table",comment="Silver shipment table",table_properties={"quality":"silver"})
def silver_shipment_table():
    return spark.read.table("enriched_shipment_vw")

@dp.table(name="silver_master_city_table",comment="Silver mastercity table",table_properties={"quality":"silver"})
def master_city():
    return spark.read.table("etl_pipeline_catalog.etl_pipeline_schema.bronze_master_city_table")

