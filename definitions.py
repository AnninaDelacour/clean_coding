from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)

from assets.a_bronze.bronze_data import bronze_data



#________________ BRONZE

load_bronze_data_job = define_asset_job(
    name="load_bronze_data_job",
    selection=AssetSelection.assets(bronze_data)
)

#________________ SILVER




#________________ GOLD



defs = Definitions(
    assets=[bronze_data],
    jobs=[load_bronze_data_job]
)
