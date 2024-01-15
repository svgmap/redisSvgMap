import unittest
import os
import io
import json
import sys
from unittest import mock
from flaskmain import app, getData, redisRegistThread
from unittest.mock import MagicMock, patch
import redis
import pickle

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

  @patch("scripts.csv2redis.Csv2redisClass.registSchema", autospec=True)
  @patch("scripts.csv2redis.Csv2redisClass.__init__", autospec=True)
  def test_buildLayer(self,mock_c2rini, mock_c2rreg):
    mock_c2rini.return_value = None
    mock_c2rreg.return_value = True
    postData = {'schema': ['title', 'metadata', 'latitude', 'longitude'], 'type': [2, 2, 1, 1], 'latCol': 2, 'lngCol': 3, 'titleCol': 0, 'idCol': -1, 'namespace': 'abcdefg_', 'name': 'sample', 'created': 1703751389604, 'defaultIcon': 8, 'defaultIconPath': 'pngs/pin_red_cross.png'}
    response = self.main.post("/svgmap/buildLayer", data=json.dumps(postData), content_type='application/json')
    self.assertEqual(response.status_code, 200)
    response.close()

  @patch("scripts.csv2redis.Csv2redisClass.__init__", autospec=True)
  def test_access2IndexFile(self, mock_c2rini):
    mock_c2rini.return_value = None
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

  @patch("scripts.csv2redis.Csv2redisClass.__init__", autospec=True)
  def test_access2StaticImageFile(self, mock_c2rini):
    mock_c2rini.return_value = None
    response = self.main.get("/svgmap/pngs/pin_yellow.png")
    self.assertEqual(response.status_code, 200)
    response.close()
    response = self.main.get("/svgmap/gps.png")
    self.assertEqual(response.status_code, 200)
    response.close()

  @patch("scripts.csv2redis.Csv2redisClass.saveSvgMapTileN", autospec=True)
  @patch("scripts.csv2redis.Csv2redisClass.init", autospec=True)
  @patch("scripts.csv2redis.Csv2redisClass.__init__", autospec=True)
  def test_access2LowResImageFile(self, mock_c2rini, mock_c2rinit, mock_c2rsave):
    mock_c2rini.return_value = None
    mock_c2rinit.return_value = None # 問答無用でNoneにしてmockしてしまっているので、Mockの仕方がきれいじゃない
    mock_c2rsave.return_value = ""
    with open("./flask/webApps/Container.svg", "r") as f:
      mock_c2rsave.return_value = f.read()
    response = self.main.get("/svgmap/temporary/svgMapTileDB.svg")
    self.assertEqual(response.status_code, 200)
    response.close()
  
  @patch("pickle.loads", autospec=True)
  @patch("redis.Redis", autospec=True)
  @patch("scripts.csv2redis.Csv2redisClass", autospec=True)
  def test_editPost(self, mock_c2r, mock_redis, mock_pickle):
    mock_redis.exists.return_value = True
    schemaObj = {
        'schema':['Name:s', 'latitude', 'longitude'],
        'type': [0,1,1],
        "latCol": 1,
        "lngCol": 2,
        "titleCol": 0,
        "idCol": -1,
        "namespace": "test_",
        "name": "default"
    }
    
    mock_pickle.return_value = schemaObj
    postData = {"action": "ADD", "to": [{"latitude":36, "longitude":139, "metadata":"aaaaa,bbb"}]}
    response = self.main.post("/svgmap/editPoint", data=json.dumps(postData), content_type='application/json')
    self.assertEqual(response.status_code, 200)

  def test_getData(self):
    schemaObj = {
        'schema':['Name:s', 'latitude', 'longitude'],
        'type': [0,1,1],
        "latCol": 1,
        "lngCol": 2,
        "titleCol": 0,
        "idCol": -1,
        "namespace": "test_",
        "name": "default"
    }
    poiData = [{"latitude":36, "longitude":139, "metadata":"aaaaa,bbb"}]
    print(getData(poiData, schemaObj))  # 何が正しい返り値か理解できてないため、未完成

  @patch("flaskmain.Csv2redisClass", autospec = True)
  def test_access2svgFile_throwException(self, mock_c2r):
    mock_c2r.return_value.saveSvgMapTileN.side_effect = Exception("saveSvgMapTileNException")
    # saveSvgMapTileN関数でExceptionを発生させる
    response = self.main.get("/svgmap/temporary/svgMapTileDB.svg")
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.data, b"TileGeneratedError")
    response = self.main.get("/svgmap/temporary/svgMapTileDBBB.png")
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.data, b"ImageOfTileGeneratedError")
    response = self.main.get("/svgmap/temporary/svgMapRoot.svg")
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.data, b"RootFileGeneratedError")
