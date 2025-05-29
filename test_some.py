import settings
from database import DataBaseFront
from local_filterer import Filterer
from datetime import datetime
import os
from main import pre_setup, config_logging, load_cached_taxon_resolution_results, process_file_faster

def test_existing_rows():
    ### Pre filter original data
    filterer = Filterer("./shapefile_cat/shapefile_cat.shp","/home/webuser/dev/python/gbif_downloader/gbif.filtered.with.quotes.2025-05-16.txt")
    filterer.filter_shapefile_limit(outside_file="outside_points.csv", inside_file="inside_points.csv")
    filterer.split_citacions(input_file="inside_points.csv",output_point_file="inside_points_p.csv",output_grid_file="inside_points_grid.csv")
    database = DataBaseFront(settings, None)
    present_points = database.get_present_ids(file_name="inside_points_p.csv", table="citacions")
    present_grids = database.get_present_ids(file_name="inside_points_grid.csv", table="citacions_10")

    filterer = Filterer("./shapefile_grid/grid_10000.shp", "/home/webuser/dev/python/gbif_downloader/inside_points_grid.csv")
    filterer.generate_grid_table(out_file="point_to_grid_mapping.csv")

    hash_present_point = list(present_points).pop()
    hash_present_grid = list(present_grids).pop()

    assert database.row_already_exists(hash_present_point)
    assert database.row_already_exists_10_10(hash_present_grid)

