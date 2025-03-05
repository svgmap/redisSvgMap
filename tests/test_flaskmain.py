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
import fakeredis

class TestOfStaticFile(unittest.TestCase):
  def setUp(self):
    self.main = app.test_client()

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
  
  def test_access2IndexFile(self):
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


class TestOfFlaskApps(unittest.TestCase):
  mk_fakeredis = fakeredis.FakeStrictRedis()

  @patch("redis.Redis", return_value=mk_fakeredis)
  def setUp(self, mock_redis):
    self.main = app.test_client()
    # 試験用のスキーマ
    postData = {'schema': ['title', 'metadata', 'latitude', 'longitude'], 'type': [2, 2, 1, 1], 'latCol': 2, 'lngCol': 3, 'titleCol': 0, 'idCol': -1, 'namespace': 's2_', 'name': 'sample', 'created': 1703751389604, 'defaultIcon': 8, 'defaultIconPath': 'pngs/pin_red_cross.png'}
    response = self.main.post("/svgmap/buildLayer", data=json.dumps(postData), content_type='application/json')
    response.close()
    # ポイントの登録
    postData = {"action": "ADD", "to": [{"latitude":36.0001, "longitude":139.0001, "metadata":"xxxx,yyy"}]}
    response = self.main.post("/svgmap/s2_/editPoint", data=json.dumps(postData), content_type='application/json')
    response.close()
    # 登録スレッドが完了するまで待機
    from flaskmain import redisRegistJob
    redisRegistJob.join()

  @patch("redis.Redis", return_value=mk_fakeredis)
  def tearDown(self, mock_redis):
    self.mock_redis = mock_redis
    self.mk_fakeredis.flushall()  

  @patch("redis.Redis", return_value=mk_fakeredis)
  def test_buildLayer(self, mock_redis):
    postData = {'schema': ['title', 'metadata', 'latitude', 'longitude'], 'type': [2, 2, 1, 1], 'latCol': 2, 'lngCol': 3, 'titleCol': 0, 'idCol': -1, 'namespace': 'temp_', 'name': 'sample', 'created': 1703751389604, 'defaultIcon': 8, 'defaultIconPath': 'pngs/pin_red_cross.png'}
    response = self.main.post("/svgmap/buildLayer", data=json.dumps(postData), content_type='application/json')
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.data, b"OK")

  @patch("redis.Redis", return_value=mk_fakeredis)
  def test_buildLayerDuplicatedError(self, mock_redis):
    postData = {'schema': ['title', 'metadata', 'latitude', 'longitude'], 'type': [2, 2, 1, 1], 'latCol': 2, 'lngCol': 3, 'titleCol': 0, 'idCol': -1, 'namespace': 's2_', 'name': 'sample', 'created': 1703751389604, 'defaultIcon': 8, 'defaultIconPath': 'pngs/pin_red_cross.png'}
    response = self.main.post("/svgmap/buildLayer", data=json.dumps(postData), content_type='application/json')
    self.assertEqual(response.status_code, 200)
    self.assertEqual(response.data, b"DUPLICATED ERROR")
    response.close()

  @patch("redis.Redis", return_value=fakeredis.FakeStrictRedis())
  def test_access2LowResImageFile(self, mock_redis):
    # with open("./flask/webApps/Container.svg", "r") as f:
    #   mock_c2r.return_value.saveSvgMapTileN.return_value = f.read()
    response = self.main.get("/svgmap/temporary/svgMapTileDB.svg")
    self.assertEqual(response.status_code, 200)
    response.close()

  @patch("redis.Redis", return_value=mk_fakeredis)
  def test_addPoi(self, mock_redis):
    # ポイントの登録
    postData = {"action": "ADD", "to": [{"latitude":26.0001, "longitude":129.0001, "metadata":"aaaaa,bbb"}]}
    response = self.main.post("/svgmap/s2_/editPoint", data=json.dumps(postData), content_type='application/json')
    response.close()
    self.assertEqual(response.status_code, 200)
    from flaskmain import redisRegistJob
    redisRegistJob.join()  # 登録スレッドが完了するまで待機
    self.assertEqual(self.mk_fakeredis.hlen("s2_D"), 2) # 登録件数の確認
    self.assertEqual(list(self.mk_fakeredis.hgetall("s2_D").keys())[0], b'3600010:13900010:xxxx,yyy' )
    self.assertEqual(list(self.mk_fakeredis.hgetall("s2_D").keys())[1], b'2600010:12900010:aaaaa,bbb' )

  @patch("redis.Redis", return_value=mk_fakeredis)
  def test_updatePoiSuccess(self, mock_redis):
    # ポイントの更新
    postData = {"action": "MODIFY", "to": [{"latitude":36.0001, "longitude":139.0001, "metadata":"aaaaa,ccc"}], "from": [{"latitude":36.0001, "longitude":139.0001, "metadata":"xxxx,yyy"}]}
    response = self.main.post("/svgmap/s2_/editPoint", data=json.dumps(postData), content_type='application/json')
    self.assertEqual(response.status_code, 200)

    from flaskmain import redisRegistJob
    redisRegistJob.join()  # 登録スレッドが完了するまで待機
    self.assertEqual(self.mk_fakeredis.hlen("s2_D"), 1) # 1件データが登録されたか確認
    self.assertEqual(list(self.mk_fakeredis.hgetall("s2_D").keys())[0], b'3600010:13900010:aaaaa,ccc' )

  @patch("redis.Redis", return_value=mk_fakeredis)
  def test_updatePoiFailed(self, mock_redis):
    # ポイントの更新
    postData = {"action": "MODIFY", "to": [{"latitude":36.0001, "longitude":139.0001, "metadata":"aaaaa,ddd"}], "from": [{"latitude":36.0001, "longitude":139.0001, "metadata":"aaaaa,xxx"}]}
    response = self.main.post("/svgmap/s2_/editPoint", data=json.dumps(postData), content_type='application/json')
    self.assertEqual(response.status_code, 200)

    from flaskmain import redisRegistJob
    redisRegistJob.join()  # 登録スレッドが完了するまで待機
    self.assertEqual(self.mk_fakeredis.hlen("s2_D"), 1) # 1件データが登録されたか確認
    self.assertEqual(list(self.mk_fakeredis.hgetall("s2_D").keys())[0], b'3600010:13900010:xxxx,yyy' )

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

  @patch("flaskmain.getData", return_value=[{"lat":36, "lng":139, "hkey":"aaaaa,bbb,ccc", "data": "aaaaa,bbb,ccc"}])
  @patch("flaskmain.Csv2redisClass", autospec = True)
  def test_mockredisThread(self, mock_c2r, flask_getData):      
    mock_c2r.return_value.getSchemaObject.return_value = {
        'schema':['Name:s', 'latitude', 'longitude'],
        'type': [0,1,1],
        "latCol": 1,
        "lngCol": 2,
        "titleCol": 0,
        "idCol": -1,
        "namespace": "test_",
        "name": "default"
    }
    mock_c2r.return_value.getMaxLevel.return_value = 10
    mock_c2r.return_value.registData.return_value = {"keys":"D"}
    mock_c2r.return_value.flushRegistData.return_value = {"keys":"D"}
    rThread = redisRegistThread(dsHash="s1_")
    rThread.jsStr = json.dumps({"action": "ADD", "to": [{"latitude":36, "longitude":139, "metadata":"aaaaa,bbb,ccc"}]})
    rThread.start()
    rThread.join()
    mock_c2r.return_value.registData.assert_called_with({'lat': 36, 'lng': 139, 'hkey': 'aaaaa,bbb,ccc', 'data': 'aaaaa,bbb,ccc'}, 10)
