from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(name="gold_wide_shipment_history",comment="Gold wideshipment table",table_properties={"quality":"gold"})

def gold_wide_shipment_history():
    ship_tbl=spark.sql("select * from etl_pipeline_catalog.etl_pipeline_schema.silver_shipment_table")
    staff_tbl=spark.sql("select * from etl_pipeline_catalog.etl_pipeline_schema. silver_staff_table").withColumnRenamed("shipment_id", "staff_shipment_id").withColumnRenamed("vehicle_type","staff_vehicle_type").withColumnRenamed("_rescued_data","staff_rescued_data")
    mc_tbl=spark.sql("select * from etl_pipeline_catalog.etl_pipeline_schema.silver_master_city_table").withColumnRenamed("_rescued_data","mc_rescued_data")

    df4 = ship_tbl.alias("sh").join(
        staff_tbl.alias("st"),
        col("sh.shipment_id") == col("st.staff_shipment_id"),
        "left"
    ).alias("ft").join(
        mc_tbl.alias("mc"),
        col("ft.origin_hub_city") == col("mc.city_name"),"left"
    )
    return df4

@dp.table(name="gold_shipment_curated_table",comment="Gold shipment curated table",table_properties={"quality":"gold"})

def logistics_shipment_gold_curation():
    
    logistics_shipment_temp_1=spark.sql(f'''select 
        shipment_id as log_shipment_id,
        order_id ,
        upper(source_city) as source_city ,
        destination_city ,
        shipment_status ,
        cargo_type ,
        vehicle_type as shipment_vehicle_type,
        payment_mode ,
        shipment_weight_kg,
        shipment_cost,
        concat('₹',cast(shipment_cost as string)) as shipment_cost_inr ,
        shipment_date,
        domain, 
        ingestion_timestamp,
        is_expedited,
        route_segment,
        vehicle_identifier,
        shipment_year,
        shipment_month,
        is_weekend,
        cost_per_kg,
        days_since_shipment,
        tax_amount,
        order_prefix,
        order_sequence,
        ship_year,
        ship_month,
        ship_day,
        route_lane,
        case when shipment_cost > 50000 then True else False end as is_high_value
        from etl_pipeline_catalog.etl_pipeline_schema.silver_shipment_table''')
    return logistics_shipment_temp_1
