import unittest
import os
import io
import json
import sys
import fakeredis
from unittest import mock
from csv2redis17 import Csv2redisClass

class TestOfCsv2Redis(unittest.TestCase):
  def setUp(self):
    self.faker = fakeredis.FakeStrictRedis(version=6)

    self.c2r = Csv2redisClass()
    self.c2r.set_connect(self.faker)
    self.c2r.init("test_")
    self.c2r.targetDir = "tests/"
    # スキーマを準備 リーダーの作成はCsv２RedisClassに不要な関数（どっかのタイミングで削除したい）
    self.file = self.c2r.getCsvReader("./worldcitiespop_jp.csv")
    header = next(self.file)
    self.csvSchemaObj = self.c2r.getSchema(header)
    self.csvSchemaObj["namespace"] = self.c2r.ns
    # スキーマを登録
    self.c2r.registSchema(self.csvSchemaObj)

  def tearDown(self):
    #self.faker.close()
    self.c2r.closeCsvReader()

  def test_displaySchemaObj(self):
    # CSVファイルを読み込みデータをredisに登録する
    correctOfSchema = ['Country:e', 'Name:s', 'AccentCity:s', 'Region:e', 'Population:n', 'latitude', 'longitude', 'Test3:n', 'Prefecture:e', 'RegistTime:n']
    correctOfType=[0, 2, 2, 0, 1, 1, 1, 1, 0, 1]
    self.assertEqual(self.c2r.schemaObj.get("schema"), correctOfSchema)
    self.assertEqual(self.c2r.schemaObj.get("type"), correctOfType)
    print(self.faker.keys())
    print(self.faker.type("test_schema"))
    print(self.faker.hget("dataSet","*"))

  def test_registData(self):
    # CSVファイルを読み込みデータをredisに登録する
    latCol = self.c2r.schemaObj.get("latCol")
    lngCol = self.c2r.schemaObj.get("lngCol")
    self.c2r.readAndRegistData(self.file, latCol, lngCol, self.c2r.maxLevel)
    print(self.faker.keys())

  # どこでデータを登録しているのか不明（高木さんに聞くこと）

  def test_buildMapData(self):
    self.c2r.buildAllLowResMap()

  def test_saveAllSvgMap(self):
    self.c2r.saveAllSvgMap(True)
    
  def test_getGeoHashCode(self):
    print(self.c2r.getGeoHashCode(130, 39, 130.01, 42, 0.1, 0.1))