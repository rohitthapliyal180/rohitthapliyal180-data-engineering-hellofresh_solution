import logging

from pyspark.python.pyspark.shell import sqlContext
from pyspark.sql.types import StructType, StructField, StringType


def read_recipe_json(spark,input_path):
    logger = logging.getLogger("recipe logger")
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("ingredients", StringType(), True),
        StructField("url", StringType(), True),
        StructField("image", StringType(), True),
        StructField("cookTime", StringType(), True),
        StructField("recipeYield", StringType(), True),
        StructField("datePublished", StringType(), True),
        StructField("prepTime", StringType(), True),
        StructField("description", StringType(), True)

    ])
    try:
        df1 = sqlContext.read.json(input_path["recipe_0"], schema)
        df2 = sqlContext.read.json(input_path["recipe_1"], schema)
        df3 = sqlContext.read.json(input_path["recipe_2"], schema)
        recipe_df_raw = df1.union(df2).union(df3)

        if recipe_df_raw.count() > 0:
            logger.info("Reading recipe file is successful")
        else:
            logger.warning("zero records in dataframe recipe_df_raw")
    except:
        logger.error("error in reading recipe json files")

    return recipe_df_raw
