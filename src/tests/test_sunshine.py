import unittest

from entities.sunshine import (
  SunshineLocation,
)

class SunshineLocationTest(unittest.TestCase):

  def test_geopy_address_to_coordinates(self):
    mit_address = '77 Massachusetts Ave, Cambridge, MA 02139, United States'
    datetime = '01-01_17:00'
    self.assertEqual(SunshineLocation(mit_address, datetime).coordinates, (42.3592962, -71.09308333242714))

  def test_closest_sunshine_station_by_coordinates(self):
    mit_address = '77 Massachusetts Ave, Cambridge, MA 02139, United States'
    datetime = '01-01_17:00'
    self.assertEqual(SunshineLocation(mit_address, datetime).closest_station, '725090')

  def test_sunshine_energy(self):
    mit_address = '77 Massachusetts Ave, Cambridge, MA 02139, United States'
    datetime = '01-01_17:00'
    self.assertEqual(SunshineLocation(mit_address, datetime).datetime_sunshine, {'AZIMUTH': 237.0, 'ETR': 11.6, 'ETRN': 480.9, 'ZENITH': 88.6})
