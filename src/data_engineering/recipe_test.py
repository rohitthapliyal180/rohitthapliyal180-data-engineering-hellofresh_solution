from src.config.constants import input_path
from src.data_engineering.read_json import read_recipe_json
from src.unit_test.testing import assert_frame_equal


def test_read_recipe_json(spark,unit_test_data):
    raw_item = read_recipe_json(spark,input_path)
    sorted_item_1 = raw_item.toPandas().sort_index(axis=1).sort_values(raw_item.columns)
    # expected output
    desired_ot = unit_test_data['test_recipe_exp_recipe_df_raw'].toPandas().sort_index(
        axis=1).sort_values(
        raw_item.columns)
    desired_ot.reset_index(inplace=True, drop=True)
    sorted_item_1.reset_index(inplace=True, drop=True)
    # compare output and expected output
    assert_frame_equal(sorted_item_1, desired_ot, check_dtype=False)