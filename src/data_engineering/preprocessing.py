import logging

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType


def preprocessing_ingredient(spark,recipe_df_raw,ingredient):
    logger = logging.getLogger("recipe logger")
    try:
        recipe_df_ingredient = recipe_df_raw.where("ingredients like '%"+ingredient+"%'")
        logger.info("function preprocessing_ingredients completed")
    except:
        logger.error("error in function preprocessing_ingredients")
    return recipe_df_ingredient

def preprocessing_PT_to_minute(spark,recipe_df_ingredient,column_name):
    logger = logging.getLogger("recipe logger")
    try:
        recipe_df_ingredient_clean = recipe_df_ingredient.withColumn(
            column_name,
            ((F.coalesce(F.regexp_extract(column_name, r'(\d+)H', 1).cast('int'), F.lit(0)) * 3600 +
            F.coalesce(F.regexp_extract(column_name, r'(\d+)M', 1).cast('int'), F.lit(0)) * 60 +
            F.coalesce(F.regexp_extract(column_name, r'(\d+)S', 1).cast('int'), F.lit(0)))/60).cast(IntegerType())
        )
        logger.info("function preprocessing_PT_to_minute completed")
    except:
        logger.error("error in function preprocessing_PT_to_minute")

    return recipe_df_ingredient_clean



