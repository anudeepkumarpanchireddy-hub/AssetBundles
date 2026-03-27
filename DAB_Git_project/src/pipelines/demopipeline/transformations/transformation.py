from pyspark.sql.functions import pipelines as dp
from pyspark.sql.types import *

from pyspark.sql.functions import *


@dp.table(name = "mycatalogdata.default.bronze_rider_vehicle_details")
def bronze_rider_vehicle_details():
  return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"dev.default.rider_vehicle_details")
            )
@dp.table(name = "mycatalogdata.default.silver_rider_vehicle_details")
def read_the_Bronze_data():
    return (
        dp.readStream("dev.default.rider_vehicle_details")
        .filter(col("vehicle_make_id") == 2)
        )
    
@dp.materialized_view(name = "mycatalogdata.default.gold_rider_vehicle_details")
def read_the_Silver_data():
    return (dp.readStream("dev.default.rider_vehicle_details")
        .filter(col("vehicle_type_id") == 2).count()
           )