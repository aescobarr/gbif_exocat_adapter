import pandas as pd
import geopandas as gpd
import csv
from shapely.geometry import Point


class Filterer:
    def __init__(self, shapefile, datafile, logger):
        self.loaded_shapefile = gpd.read_file(shapefile)
        self.df = pd.read_csv(datafile, sep='\t', low_memory=False)
        self.logger=logger


    def generate_grid_table(self):
        grid = self.loaded_shapefile

        # Load the CSV with coordinates and IDs
        df = self.df

        df = df.drop(columns=['geometry','index_right','INSPIREID','COUNTRY','NATLEV','NATLEVNAME','NATCODE','NAMEUNIT','CODNUT1','CODNUT2','CODNUT3'])

        # Create point geometries from lat/lon
        geometry = [Point(xy) for xy in zip(df['decimalLongitude'], df['decimalLatitude'])]
        points = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")  # Assuming lat/lon

        # Spatial join to find which polygon contains each point
        joined = gpd.sjoin(points, grid, how='inner', predicate='within')  # only matching points

        # Extract relevant columns
        # Replace 'grid_id_column_name' with actual name in the grid shapefile (e.g., 'grid_code')
        result = joined[['gbifID', 'id']]  # example: 'row_id', 'grid_code'

        return result.set_index('gbifID').T.to_dict('index')


    def filter_shapefile_limit(self, outside_file, inside_file):
        points = [Point(xy) for xy in zip(self.df['decimalLongitude'], self.df['decimalLatitude'])]
        gdf = gpd.GeoDataFrame(self.df, geometry=points)
        gdf.set_crs(self.loaded_shapefile.crs, inplace=True)

        self.logger.info("Shapefile crs {}".format(self.loaded_shapefile.crs))
        self.logger.info("CSV crs {}".format(gdf.crs))
        self.logger.info("Geometry type shapefile {}".format(self.loaded_shapefile.geom_type.unique()))

        # Spatial join: Check if point is inside the shapefile geometry
        # `op='within'` is now deprecated; use `predicate='within'`
        joined = gpd.sjoin(gdf, self.loaded_shapefile, how='left', predicate='within')

        self.logger.info("Number of points outside {}".format(joined['index_right'].isna().sum()))  # Number of points outside
        self.logger.info("Number of points inside {}".format(joined['index_right'].notna().sum()))  # Number of points inside

        # Classify: If joined value is NaN, point is outside
        inside = joined[joined['index_right'].notna()]
        inside.to_csv(inside_file, index=False, sep='\t', quotechar='"')

        outside = joined[joined['index_right'].isna()]
        outside.to_csv(outside_file, index=False, sep='\t', quotechar='"')


    def split_citacions(self, input_file, output_point_file, output_grid_file):
        rows_point = []
        rows_grid = []
        header = None
        with open(input_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='\t', quotechar='"')
            first = True
            for row in csv_reader:
                if first:
                    header = row
                    first = False
                else:
                    if row[23] and row[23] != '' and row[23] != 'NA' and float(row[23]) >= 1000:
                        rows_grid.append(row)
                    else:
                        rows_point.append(row)

        with open(output_point_file, "w", newline="") as f:
            writer = csv.writer(f, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            writer.writerows(rows_point)

        with open(output_grid_file, "w", newline="") as f:
            writer = csv.writer(f, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            writer.writerows(rows_grid)
