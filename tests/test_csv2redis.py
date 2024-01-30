import unittest
import os
import io
import json
import sys
import fakeredis
from unittest import mock
from unittest.mock import MagicMock, patch
from scripts.csv2redis import Csv2redisClass

class TestOfCsv2Redis(unittest.TestCase):
  def setUp(self):
    self.f_redis = fakeredis.FakeStrictRedis(version=6)

    self.c2r = Csv2redisClass()
    self.c2r.set_connect(self.f_redis)
    self.c2r.targetDir = "flask/webApps/temporary/"
    self.c2r.init("test_")
    # スキーマを準備 リーダーの作成はCsv２RedisClassに不要な関数（どっかのタイミングで削除したい）
    self.file = self.c2r.getCsvReader("./worldcitiespop_jp.csv")
    header = next(self.file)
    self.csvSchemaObj = self.c2r.getSchema(header)
    self.csvSchemaObj["namespace"] = self.c2r.ns
    # スキーマを登録
    self.c2r.registSchema(self.csvSchemaObj)

  def tearDown(self):
    self.f_redis.close()
    self.c2r.closeCsvReader()
    pass

  def test_displaySchemaObj(self):
    # Schemaが登録されているかの確認
    correctOfSchema = ['Country:e', 'Name:s', 'AccentCity:s', 'Region:e', 'Population:n', 'latitude', 'longitude', 'Test3:n', 'Prefecture:e']
    correctOfType=[0, 2, 2, 0, 1, 1, 1, 1, 0]
    self.assertEqual(self.c2r.schemaObj,{
        'schema':['Country:e', 'Name:s', 'AccentCity:s', 'Region:e', 'Population:n', 'latitude', 'longitude', 'Test3:n', 'Prefecture:e'],
        'type': [0, 2, 2, 0, 1, 1, 1, 1, 0],
        "latCol": 5,
        "lngCol": 6,
        "titleCol": 1,
        "idCol": -1,
        "namespace": "test_",
        "name": "default"
    })
    self.assertEqual(self.c2r.schemaObj.get("schema"), correctOfSchema)
    self.assertEqual(self.c2r.schemaObj.get("type"), correctOfType)

  def test_readAndRegistData(self):
    # CSVファイルを読み込みデータをredisに登録する
    latCol = self.c2r.schemaObj.get("latCol")
    lngCol = self.c2r.schemaObj.get("lngCol")
    self.c2r.readAndRegistData(self.file, latCol, lngCol, 16)
    result = self.f_redis.hgetall('test_DBADBADADBA')
    #print(self.f_redis.hgetall('test_DBADBADADBA'))
    # TODO 登録後のデータ正常性がまだ未確認
    #print(result[b'jp,aragachi,Aragachi,47,-,2611777,12769111,26.117778,okinawa,'])
    self.assertEqual(len(self.f_redis.keys("test_*")), 137)
    # ハッシュキーを作成する際getOneData関数で緯度経度を丸めてます
    print(result)
    self.assertEqual(result[b"2611777:12769111:jp,aragachi,Aragachi,47,-,26.117778,okinawa"].decode("UTF-8"), "jp,aragachi,Aragachi,47,-,26.117778,127.691111,26.117778,okinawa")
  
  def test_buildMapData(self):
    # TODO テスト内容はこれから
    latCol = self.c2r.schemaObj.get("latCol")
    lngCol = self.c2r.schemaObj.get("lngCol")
    self.c2r.readAndRegistData(self.file, latCol, lngCol, 16)
    self.c2r.buildAllLowResMap()

  def test_saveAllSvgMap(self):
    # TODO テスト内容はこれから
    latCol = self.c2r.schemaObj.get("latCol")
    lngCol = self.c2r.schemaObj.get("lngCol")
    self.c2r.readAndRegistData(self.file, latCol, lngCol, 16) # 登録
    self.c2r.buildAllLowResMap() # POIが上限あふれたラスターデータの出力
    self.c2r.saveAllSvgMap(True) # ベクトルデータ出力

  def test_getGeoHashCode(self):
    self.assertEqual(self.c2r.getGeoHashCode(130, 39, 130.01, 42, 0.1, 0.1), ('A', 130.01, 42, 0.05, 0.05))
    
  def test_registData(self):
    # self.c2r.burstSize = 3
    correct =  {"success": -1, "keys": []}
    data_case = [{'lat': 26.606111, 'lng': 127.923889, 'data': 'jp,a,A,47,,26.606111,127.923889,26.606111,okinawa', 'hkey': 'jp,a,A,47,,2660611,12792388,26.606111,okinawa'}]
    result = self.c2r.registData(data_case, 3)
    self.assertEqual(result, correct)
    
    data_case = [
      {'lat': 26.606111, 'lng': 127.923889, 'data': 'jp,a,A,47,-,26.606111,127.923889,26.606111,okinawa,', 'hkey': 'jp,a,A,47,-,2660611,12792388,26.606111,okinawa,'},
      {'lat': 26.606111, 'lng': 127.923889, 'data': 'jp,a,A,47,-,26.606111,127.923889,26.606111,okinawa,', 'hkey': 'jp,a,A,47,-,2660611,12792388,26.606111,okinawa,'},
      {'lat': 26.606111, 'lng': 127.923889, 'data': 'jp,a,A,47,-,26.606111,127.923889,26.606111,okinawa,', 'hkey': 'jp,a,A,47,-,2660611,12792388,26.606111,okinawa,'},
      {'lat': 26.606111, 'lng': 127.923889, 'data': 'jp,a,A,47,-,26.606111,127.923889,26.606111,okinawa,', 'hkey': 'jp,a,A,47,-,2660611,12792388,26.606111,okinawa,'},
    ]
    result = self.c2r.burstRegistData(data_case, 3)
    correct = {'keys': ['D', 'D', 'D', 'D'], 'success': 4}
    self.assertEqual(result, correct)

    #self.assertEqual(self.f_redis.get("test_D"),"string")
    
  def test_getOneData(self):
    # デフォルト：データをすべて使用してハッシュキー（ユニークキー）を生成するパターン
    data = ['jp','a','A','47','','26.606111','127.923889','26.606111','okinawa']
    result = self.c2r.getOneData(data, 5, 6)  # 5:lat, 6:lng
    correct = {'lat': 26.606111, 'lng': 127.923889, 'data': 'jp,a,A,47,-,26.606111,127.923889,26.606111,okinawa', 'hkey': '2660611:12792388:jp,a,A,47,-,26.606111,okinawa'}
    self.assertDictEqual(result, correct)
    # HashKeyを指定するパターン
    result = self.c2r.getOneData(data, 5, 6, 8)  # 5:lat, 6:lng
    self.assertDictEqual(result, correct)

  def test_getPoiKey(self):
     # デフォルト：データをすべて使用してハッシュキー（ユニークキー）を生成するパターン
    data = ['jp','a','A','47','','26.606111','127.923889','26.606111','okinawa']
    schema = [0, 2, 2, 0, 1, 1, 1, 1, 0]
    result = self.c2r.getPoiKey(data, 5, 6, schema)  # 5:lat, 6:lng
    correct = {'lat': 26.606111, 'lng': 127.923889, 'hkey': '2660611:12792388:jp,a,A,47,,26.606111,okinawa'}

    self.assertEqual(result, correct)

  @patch("pickle.loads", MagicMock(side_effect=Exception()))
  def test_saveSvgMapTileN_throwException(self):
    self.c2r.schemaObj = {
        'schema':['Country:e', 'Name:s', 'AccentCity:s', 'Region:e', 'Population:n', 'latitude', 'longitude', 'Test3:n', 'Prefecture:e'],
        'type': [0, 2, 2, 0, 1, 1, 1, 1, 0],
        "latCol": 5,
        "lngCol": 6,
        "titleCol": 1,
        "idCol": -1,
        "namespace": "test_",
        "name": "default"
    }
    # pickle.loadsでエラー出たとしても返す値がないため上位でエラー処理する必要あり
    # そのためこのファイルでは試験の正常性判定してません。
    result = self.c2r.saveSvgMapTileN(geoHash=None, dtype=None, lowResImage=False, onMemoryOutput=True)  # 5:lat, 6:lng
