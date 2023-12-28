import unittest
import os
import io
import json
import sys
from unittest import mock
from flaskmain import app

class TestOfFlaskApps(unittest.TestCase):
  def setUp(self):
    self.main = app.test_client();

  def test_access2RootPath(self):
    response = self.main.get("/")
    self.assertEqual(response.status_code, 200)
    
  def test_access2QueryPath(self):
    response = self.main.get("/query?hello=b")
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.data.decode("UTF-8"), "QUERY:hello=b : b")
  
  def test_access2rtDatasetPath(self):
    response = self.main.get("/dataset/show")
    self.assertEqual(response.status_code, 200)

  def test_buildLayer(self):
    postData = {'schema': ['title', 'metadata', 'latitude', 'longitude'], 'type': [2, 2, 1, 1], 'latCol': 2, 'lngCol': 3, 'titleCol': 0, 'idCol': -1, 'namespace': 'abcdefg_', 'name': 'sample', 'created': 1703751389604, 'defaultIcon': 8, 'defaultIconPath': 'pngs/pin_red_cross.png'}
    response = self.main.post("/svgmap/buildLayer", data=json.dumps(postData), content_type='application/json')
    self.assertEqual(response.status_code, 200)
    response.close()

  def test_access2IndexFile(self):
    print("static index file ")
    response = self.main.get("/svgmap/index.html")
    self.assertEqual(response.status_code, 200)
    response.close()
    response = self.main.get("/svgmap/")
    self.assertEqual(response.status_code, 200)
    response.close()
    response = self.main.get("/svgmap")
    self.assertEqual(response.status_code, 200)
    response.close()

  def test_access2StaticImageFile(self):
    response = self.main.get("/svgmap/pngs/pin_yellow.png")
    self.assertEqual(response.status_code, 200)
    response.close()
    response = self.main.get("/svgmap/gps.png")
    self.assertEqual(response.status_code, 200)
    response.close()


  def test_access2LowResImageFile(self):
    response = self.main.get("/svgmap/temporary/svgMapTileDB.svg")
    self.assertEqual(response.status_code, 200)
    response.close()