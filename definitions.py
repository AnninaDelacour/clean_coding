from dagster import (
    Definitions,
    define_asset_job,
    AssetSelection,
)

from assets.a_bronze.bronze_data import bronze_data, check_transaction_id_not_null
from assets.b_silver.silver_data_null_handling import silver_null_handling, silver_data_has_no_nulls
from assets.b_silver.silver_data_filtered import silver_filtered, silver_data_check_product_value
from assets.b_silver.silver_data_sorted import silver_sorted, silver_data_is_sorted
from assets.b_silver.silver_data_nan_value_handling import silver_nan_and_validated, silver_data_validated
from assets.c_gold.gold_data_for_db import gold_data_db, gold_data_db_validated
from assets.c_gold.gold_data_for_ml import gold_data_ml, gold_data_ml_validated


#________________ BRONZE

load_bronze_data_job = define_asset_job(
    name="load_bronze_data_job",
    selection=AssetSelection.assets(bronze_data, check_transaction_id_not_null)
)

#________________ SILVER

load_silver_data_job = define_asset_job(
    name="load_silver_data_job",
    selection=AssetSelection.assets(silver_null_handling,silver_data_has_no_nulls,
                                    silver_filtered, silver_data_check_product_value,
                                    silver_sorted, silver_data_is_sorted,
                                    silver_nan_and_validated, silver_data_validated)
)

#________________ GOLD

load_gold_data_job = define_asset_job(
    name="load_gold_data_job",
    selection=AssetSelection.assets(gold_data_db, gold_data_db_validated,
                                    gold_data_ml, gold_data_ml_validated)
)

#________________ ASSET PIPELINE

defs = Definitions(
    assets=[bronze_data, silver_null_handling, silver_filtered, silver_sorted, silver_nan_and_validated, gold_data_db, gold_data_ml],
    asset_checks=[check_transaction_id_not_null, silver_data_has_no_nulls, silver_data_check_product_value, 
                  silver_data_is_sorted, silver_data_validated, gold_data_db_validated, gold_data_ml_validated],
    jobs=[load_bronze_data_job, load_silver_data_job, load_gold_data_job]
)