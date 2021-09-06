from src.data_engineering.calculation import cooking_time, avg_cooking_time, write_to_csv
from src.config.constants import user_unit_test_path, input_dict, input_path
from src.data_engineering.preprocessing import preprocessing_ingredient, preprocessing_PT_to_minute
from src.data_engineering.read_json import read_recipe_json
from pyspark.sql import SparkSession
import logging


def enable_logging():
    # Create and configure logger
    logging.basicConfig(filename="C:/projects/hello_fresh/logs/recipe.log",
                        level=logging.INFO,
                        format='%(asctime)s-%(levelname)s: %(message)s',
                        filemode='w')
    # Creating an object
    logger = logging.getLogger("recipe logger")

    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)

    return logger

def get_configured_session():
    logger = logging.getLogger("recipe logger")
    try:
        spark = SparkSession.builder.enableHiveSupport() \
            .config("hive.exec.dynamic.partition", "true") \
            .config("hive.exec.dynamic.partition.mode", "nonstrict") \
            .getOrCreate()
        logger.info("Spark session started")
    except:
        logger.error("error in creating Spark session ")

    return spark

def generate_test_data(input_dict,user_unit_test_path,file_name_prefix):
    spark = get_configured_session()
    for df in input_dict.keys():
        input_dict[df].toPandas().sort_index(axis=1).to_csv(user_unit_test_path + (file_name_prefix + df + '.csv'),
                                                               index=False)
    return None


def run_hellofresh_main(module,input_dict,input_path):
    logger = enable_logging()
    spark = get_configured_session()
    input_dict["recipe_df_raw"] = read_recipe_json(spark,input_path)
    input_dict["recipe_df_ingredient"] = preprocessing_ingredient(spark,input_dict["recipe_df_raw"],"Beef")
    input_dict["recipe_df_cooktime_minutes"] = preprocessing_PT_to_minute(spark,input_dict["recipe_df_ingredient"],"cookTime")
    input_dict["recipe_df_preptime_minutes"] = preprocessing_PT_to_minute(spark, input_dict["recipe_df_cooktime_minutes"], "prepTime")
    input_dict["recipe_cook_time"]= cooking_time(spark,input_dict["recipe_df_preptime_minutes"])
    input_dict["recipe_final"] = avg_cooking_time(spark, input_dict["recipe_cook_time"])
    write_to_csv(input_dict["recipe_final"],'C:/projects/hello_fresh/output/recipe_output.csv')




if __name__ == "__main__":
    run_hellofresh_main("recipe",input_dict,input_path)
    generate_test_data(input_dict,user_unit_test_path,"test_recipe_exp_")
    #unittest.main()
