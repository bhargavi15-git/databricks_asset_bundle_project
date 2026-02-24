from pyspark import pipelines as dp
from pyspark.sql.functions import col,lit

@dp.table(name='bronze_staff1_table',comment="Bronze staff1 table",table_properties={"quality":"bronze"})
def staff1_data():
 return spark.read.format("delta").load("/Volumes/etl_pipeline_catalog/etl_pipeline_schema/etl_bronzezone_vol/staff1").withColumn("source_cat",lit("source1"))

@dp.table(name='bronze_staff2_table',comment="Bronze staff2 table",table_properties={"quality":"bronze"})
def staff2_data():
 return spark.read.format("delta").load("/Volumes/etl_pipeline_catalog/etl_pipeline_schema/etl_bronzezone_vol/staff2").withColumn("source_cat",lit("source2"))

@dp.table(name="bronze_shipments_table",comment="Bronze shipment table",table_properties={"quality":"bronze"})
def shipments():
    return spark.read.format("delta").load("/Volumes/etl_pipeline_catalog/etl_pipeline_schema/etl_bronzezone_vol/shipment_details")

@dp.table(name="bronze_master_city_table",comment="Bronze mastercity table",table_properties={"quality":"bronze"})
def master_city():
    return spark.read.format("delta").load("/Volumes/etl_pipeline_catalog/etl_pipeline_schema/etl_bronzezone_vol/master_city")




