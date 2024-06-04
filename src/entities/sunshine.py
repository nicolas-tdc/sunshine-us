"""This file handles sunshine data by location and datetime"""

import os
import pandas as pd
import tarfile as tf
import urllib.request

from io import BytesIO
from geopy.geocoders import Nominatim
from scipy import spatial

from globals import SUNSHINE_DATA_DIR, SUNSHINE_MAPPING, SUNSHINE_DATA_URL

class SunshineLocation():
  def __init__(self, address, datetime):
    self.years = list(range(1995, 2006))
    self.data_extract = {
      'ZENITH': 2,  # (deg)
      'AZIMUTH': 3, # (deg)
      'ETR': 4,     # (W/m^2)
      'ETRN': 5,    # (W/m^2)
    }
    self.address = address
    self.coordinates = self.coordinates_from_address()
    self.closest_station = self.closest_station()
    self.station_filepath = self.save_sunshine_data()
    self.datetime_sunshine = self.datetime_sunshine(datetime)

  def coordinates_from_address(self):
    """
    :return: set - Coordinates for a given address or raise ValueError if address not found
    """
    geolocator = Nominatim(user_agent="ge_sunshine")

    location = geolocator.geocode(self.address)
    if location:
      return (location.latitude, location.longitude)
    else:
      raise ValueError('Unknown address.')

  def closest_station(self):
    """
    :return: string - Closest station's mapping code
    """
    sunshine_mapping = SUNSHINE_MAPPING

    # List of stations coordinates
    mapping_file = open(sunshine_mapping)
    df = pd.read_csv(mapping_file)
    stations = zip(df.lat, df.lng)
    stations_list = list(stations)

    # Compare coordinates to find closest station
    tree = spatial.KDTree(stations_list)
    dd, closest_index = tree.query([self.coordinates])
    closest_station = df.iloc[closest_index[0]]

    return str(closest_station.code)

  def datetime_sunshine(self, datetime):
    """
    :param datetime: string
    :return: dict - Sunshine data by datetime
    """
    datetime_sunshine = {}

    # Get corresponding file from closest location's code
    station_file = open(self.station_filepath)
    df = pd.read_csv(station_file)

    # Find row from datetime and return sunshine values
    data = df.query('date_time == @datetime')
    for label, column_number in self.data_extract.items():
      datetime_sunshine[label] = data[label].values[0]

    return datetime_sunshine

  def save_sunshine_data(self):
    """
    :return: str - Creates sunshine datas clean csv files and returns path
    """
    sunshine_data_url = SUNSHINE_DATA_URL

    sunshine_data_dir = SUNSHINE_DATA_DIR
    if not os.path.exists(sunshine_data_dir):
        os.makedirs(sunshine_data_dir)

    station_filepath = './' + sunshine_data_dir + '/' + self.closest_station + '.csv'
    if not os.path.isfile(station_filepath):
      datas_url = sunshine_data_url + self.closest_station + '.tar.gz'

      url_status_code = urllib.request.urlopen(datas_url).getcode()
      if url_status_code == 200:
        # Open archive from valid url
        datas_archive = urllib.request.urlopen(datas_url).read()

        filebytes = BytesIO(datas_archive)
        try:
          # Extract tar csv data from external url
          tar = tf.open(fileobj=filebytes, mode='r:gz')
          data_extract = self.extract_tar_csv_files_data(tar)
          # Write clean csv
          csv_columns = ['date_time']

          csv_data = {'date_time': data_extract['datetime']}
          for label, column_number in self.data_extract.items():
            csv_columns.append(label)
            csv_data[label] = data_extract[label]

          with open(station_filepath, 'w') as f:
            new_df = pd.DataFrame(csv_data, columns=csv_columns)
            new_df.to_csv(station_filepath, index=False, header=True)

        except tf.TarError:
          print('Invalid Tar file')
      else:
        raise ValueError('Invalid URL for sunshine datas.')

    return station_filepath

  def extract_tar_csv_files_data(self, tar):
    """
    :param tar: TarFile
    :return: dict with extracted data from external url for clean CSV generation
    """
    data_extract = {'datetime': []}
    for label, column_number in self.data_extract.items():
      data_extract[label] = []
    years_count = len(self.years)
    all_files = self.extract_and_concatenate_csv_files(tar)

    # Get dates, times and mean values from concatenated CSV Files
    rows_count = int(len(all_files.index) / years_count - 1)
    for i in range(1, rows_count + 1):
      date = all_files.at[i, 0].replace(str(self.years[0]) + '-', '')
      time = all_files.at[i, 1]

      data_extract['datetime'].append(date + '_' + time)
      for label, column_number in self.data_extract.items():

        item_value = 0
        for year_i, year in enumerate(self.years):

          str_value = all_files.at[i + rows_count * year_i + year_i, column_number]
          if '.' in str_value:
            item_value += float(str_value)
          else:
            item_value += int(str_value)
        data_extract[label].append(round(item_value / years_count, 1))

    return data_extract

  def extract_and_concatenate_csv_files(self, tar):
    """
    :param tar: Tarfile
    :return: DataFrame - Concatenated CSV files
    """
    csv_files_path = 'nsrdb_solar/' + self.closest_station

    csv_files = []
    for year in self.years:
      year_csv_path = csv_files_path + '/' + self.closest_station + '_' + str(year) + '.csv'
      df = pd.read_csv(tar.extractfile(tar.getmember(year_csv_path)), header=None)
      # Remove February 29th from leap years
      leap_extra_day = str(year) + '-02-29'
      df = df[df.iloc[:, 0] != leap_extra_day]
      csv_files.append(df)

    return pd.concat(csv_files, ignore_index=True)
