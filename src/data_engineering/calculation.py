import logging

from pyspark.sql.functions import when
from pyspark.sql.types import IntegerType


def cooking_time(spark,recipe_df):
    logger = logging.getLogger("recipe logger")
    try:
        recipe_df_tct = recipe_df.withColumn(
            "total_cook_time",
            (recipe_df.cookTime + recipe_df.prepTime).cast(IntegerType()))
        logger.info("function cooking_time completed")
    except:
        logger.error("error in function cooking_time")

    return recipe_df_tct

def avg_cooking_time(spark, recipe_df):
    logger = logging.getLogger("recipe logger")
    try:
        recipe_df_final = recipe_df.withColumn("difficulty",when(recipe_df.total_cook_time < 30, 'easy')
                                               .when((recipe_df.total_cook_time >= 30) & (recipe_df.total_cook_time <= 60), 'medium')
                                               .when(recipe_df.total_cook_time > 60, 'hard'))

        recipe_df_final = recipe_df_final.groupBy("difficulty").avg("total_cook_time").withColumnRenamed('avg(total_cook_time)','total_cook_time')

        if recipe_df_final.count() > 0:
            logger.info("function avg_cooking_time completed")
        else:
            logger.warning("zero records in dataframe recipe_df_final")
    except:
        logger.error("error in function avg_cooking_time")
    return recipe_df_final


def write_to_csv(df,export_path):
    logger = logging.getLogger("recipe logger")
    try:
        df.toPandas().to_csv(export_path,index=False)
        logger.info("function write_to_csv completed")
    except:
        logger.error("error in function write_to_csv")




