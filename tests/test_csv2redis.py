import unittest
import os
import io
import json
import sys
import fakeredis
from unittest import mock
from csv2redis import Csv2redisClass

class TestOfCsv2Redis(unittest.TestCase):
  def setUp(self):
    self.f_redis = fakeredis.FakeStrictRedis(version=6)

    self.c2r = Csv2redisClass()
    self.c2r.set_connect(self.f_redis)
    self.c2r.init("test_")
    self.c2r.targetDir = "tests/temporary"
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
    correctOfSchema = ['Country:e', 'Name:s', 'AccentCity:s', 'Region:e', 'Population:n', 'latitude', 'longitude', 'Test3:n', 'Prefecture:e', 'RegistTime:n']
    correctOfType=[0, 2, 2, 0, 1, 1, 1, 1, 0, 1]
    self.assertEqual(self.c2r.schemaObj.get("schema"), correctOfSchema)
    self.assertEqual(self.c2r.schemaObj.get("type"), correctOfType)

  def test_readAndRegistData(self):
    # CSVファイルを読み込みデータをredisに登録する
    latCol = self.c2r.schemaObj.get("latCol")
    lngCol = self.c2r.schemaObj.get("lngCol")
    self.c2r.readAndRegistData(self.file, latCol, lngCol, 16)  # 8:maxlevel
    print('>>>>>>>')
    print(self.f_redis.keys())
    print(type(self.f_redis.get('test_DBADBADADBA')))
    # TODO 登録後のデータ正常性がまだ未確認
    #print('test_DBB : '+self.f_redis.get('test_DBADBADADBA').decode('utf-8'))
    #self.assertEqual(self.f_redis.get("test_DBBCCDC").decode('utf-8'),"string")
  
  def test_buildMapData(self):
    self.c2r.buildAllLowResMap()

  def test_saveAllSvgMap(self):
    self.c2r.saveAllSvgMap(True)

  def test_getGeoHashCode(self):
    self.assertEqual(self.c2r.getGeoHashCode(130, 39, 130.01, 42, 0.1, 0.1), ('A', 130.01, 42, 0.05, 0.05))
    
  def test_registData(self):
    # self.c2r.burstSize = 3
    correct =  {"success": -1, "keys": []}
    data_case = [{'lat': 26.606111, 'lng': 127.923889, 'data': 'jp,a,A,47,-,26.606111,127.923889,26.606111,okinawa,', 'hkey': 'jp,a,A,47,-,2660611,12792388,26.606111,okinawa,'}]
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
    data = ['jp','a','A','47','','26.606111','127.923889','26.606111','okinawa']
    result = self.c2r.getOneData(data, 5, 6)  # 5:lat, 6:lng
    correct = {'lat': 26.606111, 'lng': 127.923889, 'data': 'jp,a,A,47,-,26.606111,127.923889,26.606111,okinawa,', 'hkey': 'jp,a,A,47,-,2660611,12792388,26.606111,okinawa,'}
    self.assertEqual(result, correct)
