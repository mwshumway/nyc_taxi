"""Process.py: Process the data from the raw data to the processed data."""

import os
import pandas as pd
import geopandas as gpd
import h3
# from multiprocessing import Pool

# def convert_datetime(chunk):
#             chunk['pickup_datetime'] = pd.to_datetime(chunk['pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
#             return chunk

class TLCDataProcessor:
    """Class to process the TLC NYC taxi data.
    
    Using this as a base class because we may use different data processing schemes in the future.
    """
    def __init__(self, data_paths=[], minute_interval=30):        
        self.data_paths = data_paths
        self.minute_interval = minute_interval
        self.data = None
        self.pickup_date_key = 'pickup_datetime'  # Key for the pickup date column. This by default
        self.location_id_key = None  # Key for the location ID column. This should be set by the subclass. 

    def load_data(self):
        """Load the data from the data paths."""
        for data_path in self.data_paths:
            if not os.path.exists(data_path):
                raise FileNotFoundError(f"Data path {data_path} does not exist.")
            if self.data is None:
                self.data = pd.read_csv(data_path) if data_path.endswith(".csv") else pd.read_parquet(data_path)
            else:
                self.data = pd.concat([self.data, pd.read_csv(data_path)], ignore_index=True) if data_path.endswith(".csv") else pd.concat([self.data, pd.read_parquet(data_path)], ignore_index=True)
    
    def build_date_time(self):
        """Build new colums for the day, date, and time.
        
            day (str): Day of the week
            date (str): Date in the format YYYY-MM-DD
            time (str): Time in the format HH:MM. Rounded to nearest minute_interval.
        """
        # Check if the column name is "pickup_datetime". If not, rename it.
        if "pickup_datetime" not in self.data.columns:
            if "tpep_pickup_datetime" in self.data.columns:
                self.data.rename(columns={"tpep_pickup_datetime": "pickup_datetime"}, inplace=True)
            else:
                raise ValueError("No column found for pickup datetime.")
        
        if "+00:00" in str(self.data["pickup_datetime"].iloc[0]):  # Check as string
            self.data["pickup_datetime"] = self.data["pickup_datetime"].astype(str).str.replace("+00:00", "")
        
        if " UTC" in str(self.data["pickup_datetime"].iloc[0]):  # Check as string
            self.data["pickup_datetime"] = self.data["pickup_datetime"].astype(str).str.replace(" UTC", "")

        # chunk_size = 5_000_000  
        # chunks = [self.data.iloc[i:i+chunk_size] for i in range(0, len(self.data), chunk_size)]

        # with Pool() as pool:  # Use multiprocessing to speed up the conversion
        #     results = pool.map(convert_datetime, chunks)

        # self.data = pd.concat(results, ignore_index=True)  # Concatenate the results back into a single DataFrame

        self.data["pickup_datetime"] = pd.to_datetime(self.data["pickup_datetime"], format='%Y-%m-%d %H:%M:%S')

        # Get day of the week
        self.data["day"] = self.data["pickup_datetime"].dt.day_name()

        # Get date
        self.data["date"] = self.data["pickup_datetime"].dt.date

        # Get time rounded to the nearest minute_interval - and only keep the hours and minutes
        self.data["time"] = self.data["pickup_datetime"].dt.round(f"{self.minute_interval}min").dt.time

        # Drop the original datetime column
        self.data.drop(columns=["pickup_datetime"], inplace=True)

    def count_pickups(self):
        """Count the number of pickups at each location ID at each time interval. 
        So one row will correspond to a location ID, day, and time. The columns will be:
            - location ID
            - count of pickups
            - day
            - time
        """
        if not self.location_id_key:
            raise ValueError("Location ID key not set.")
        
        self.data = (
            self.data
            .groupby([self.location_id_key, "day", "time"])  # Group by location ID, day, and time
            .size()                                    # Count the number of instances in each group
            .rename("pickup_count")                    # Rename the Series to pickup_count
            .reset_index()                             # Convert index columns back into regular columns
            )

    def write_data(self, output_path):
        """Write the processed data to a new file."""
        if output_path.endswith(".csv"):
            self.data.to_csv(output_path, index=False)
        elif output_path.endswith(".parquet"):
            self.data.to_parquet(output_path, index=False)
        else:
            raise ValueError(f"Output format not supported: {output_path}")


class LocationIDScheme(TLCDataProcessor):
    """Class to process data, assuming that the data contains location IDs and not the actual lat/lon coordinates."""
    def __init__(self, data_path, minute_interval=30):
        super().__init__(data_path, minute_interval)
        self.minute_interval = minute_interval  # Round times to the nearest minute_interval
        self.location_id_key = "PULocationID"  # Key for the location ID column
    
    
    def correct_location_ids(self):
        """Correct the location IDs that are not contained in the zones.shp file.
        
        103, 104, 105 are all the same location. Assign all to 103.
        56 and 57 are the same location. Assign all to 56.
        264 is unknown. Drop these rows.
        265 is out of NYC. Drop these rows.
        """
        self.data.loc[self.data["PULocationID"].isin([104, 105]), "PULocationID"] = 103
        self.data.loc[self.data["PULocationID"] == 57, "PULocationID"] = 56

        self.data = self.data[~self.data["PULocationID"].isin([264, 265])]

    def id2coords(self):
        """Convert the location IDs to lat/lon coordinates. Create new columns for the lat/lon coordinates."""
        zones = gpd.read_file("taxi_zones/taxi_zones.shp")  # Load the taxi zone shapefile
        zones = zones.to_crs(epsg=32618)  # Convert to a projected CRS
        zones['centroid'] = zones['geometry'].centroid
        zones = zones.to_crs(epsg=4326)  # Convert back to WGS84

        # UserWarning: Geometry is in a geographic CRS. Results from 'centroid' are likely incorrect. Use 'GeoSeries.to_crs()' to re-project geometries to a projected CRS before this operation.
        # Note sure where this is coming from. Get back to this later.

        # Get the lat/lon coordinates
        zones['centroid'] = zones['geometry'].centroid
        zones['lon'] = zones['centroid'].x
        zones['lat'] = zones['centroid'].y

        zones = zones[["LocationID", "lon", "lat"]]

        # Merge the data with the zones data to get the lat/lon coordinates
        self.data = self.data.merge(zones, left_on="PULocationID", right_on="LocationID", how="left")

        # Drop the LocationID column
        self.data.drop(columns=["LocationID"], inplace=True)
    

class LocationCoordScheme(TLCDataProcessor):
    """Class to process data, assuming that the data contains the actual lat/lon coordinates.
    
    Uses Uber's H3 library to convert the lat/lon coordinates to location IDs.
    """
    
    def __init__(self, data_path, minute_interval=30, resolution=9):
        super().__init__(data_path, minute_interval)
        self.location_id_key = 'H3LocationID'  # Key for the location ID column
        self.minute_interval = minute_interval  # Round times to the nearest minute_interval
        self.resolution = resolution  # Resolution for the H3 location ID
    
    def coords2id(self):
        """Convert the lat/lon coordinates to location IDs. Create new columns for the location IDs."""
        self.data[self.location_id_key] = self.data.apply(lambda x: h3.latlng_to_cell(x['pickup_latitude'], x['pickup_longitude'], self.resolution), axis=1)  # Convert the lat/lon coordinates to location IDs

    



if __name__ == "__main__":
    # data_path = ['raw_data/yellow.parquet']
    # processor = LocationIDScheme(data_path)
    # processor.load_data()
    # processor.build_date_time()
    # processor.count_pickups()
    # processor.correct_location_ids()
    # processor.id2coords()
    # processor.write_data('processed_data/yellow_processed_from_ids.parquet')
    # print('Done processing yellow taxi data.')

    data_path2 = ['raw_data/kaggle2018_train.csv', 'raw_data/kaggle2018_test.csv']
    processor2 = LocationCoordScheme(data_path2)
    processor2.load_data()
    if 'key' in processor2.data.columns:
            processor2.data.drop(columns=['key'], inplace=True)
    processor2.build_date_time()
    processor2.coords2id()
    processor2.count_pickups()
    processor2.write_data('processed_data/kaggle2018_processed_from_coords.parquet')
    print('Done processing kaggle 2018 data.')

