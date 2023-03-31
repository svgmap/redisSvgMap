# dbDump.py
# SVGMap/redisのデータベースの内容を、オリジナルの再利用しやすい形でファイル保存&レストアする。
# Programmed by Satoru Takagi
# History:
# 2023/02/08 1st release
# 2023/03/27 restore機能

import redis
import json
import pickle
import argparse
import os

from csv2redis17 import Csv2redisClass

# import codecs


class Csv2redisDumperClass():
  def __init__(self, dbNumb=0, outputPath="./dump"):
    self.r = redis.Redis(host='localhost', port=6379, db=dbNumb)
    self.outputPath = outputPath + "/"
    self.csv2redis = Csv2redisClass(dbNumb)

  # def getDataSet(self):
  #  ds = self.r.hgetall("dataSet")
  #  return (ds)

  def decode_dict(self, d):
    result = {}
    for key, value in d.items():
      if isinstance(key, bytes):
        key = key.decode()
      if isinstance(value, bytes):
        value = value.decode()
      elif isinstance(value, dict):
        value = self.decode_dict(value)
      result.update({key: value})
    return result

  def listSubLayers(self):
    # sl = self.getDataSet()
    sl = self.csv2redis.listSubLayers()
    # print("SubLayers:",sl)
    indexJsPath = self.outputPath + "index.json"
    # 上書き禁止モードで書き込み("w"ではなくて"x")
    indexJsFile = open(indexJsPath, mode="x", encoding="utf-8")
    json.dump(self.decode_dict(sl), indexJsFile, indent=2, ensure_ascii=False)
    indexJsFile.close()

    for key in sl:
      print("SubLayerName:", sl[key].decode(), " SubLayerKey:", key.decode())
      self.getSubLayer(key.decode())

  def getSubLayer(self, slKey):
    slSchema = self.getSchema(slKey)
    print("SCHEMA: latCol", slSchema.get("latCol"), " lngCol:", slSchema.get("lngCol"), " schema:", slSchema.get("schema"))
    # print("SCHEMA Object:::",slSchema)

    schemaPath = self.outputPath + slKey + ".json"
    schemaFile = open(schemaPath, mode="x", encoding="utf-8")
    json.dump(slSchema, schemaFile, indent=2, ensure_ascii=False)
    schemaFile.close()

    csvPath = self.outputPath + slKey + ".csv"
    csvFile = open(csvPath, mode="x", encoding="utf-8")
    # csvFile = codecs.open(csvPath,"w","utf-8")

    tkeys = self.r.keys(slKey + "[A-D]*")
    # print("TILEKEYS:::",tkeys)
    # tkeys.sort(key=len)
    for tkey in tkeys:
      if self.r.type(tkey) == b"string":
        pass
      else:  # そのタイルは実データが入っている(b"list")のデータ
        # print("This is real data:", tkey, self.r.type(tkey))
        src = list((self.r.hgetall(tkey)).values())
        #hKeys = list((self.r.hgetall( tkey)).keys())
        # print(src)
        for poidata in src:
          # print (poidata.decode())
          csvTxt = poidata.decode()
          csvTxt = csvTxt.replace('"','')
          csvTxt = csvTxt.replace('\r','')
          csvTxt = csvTxt.replace('\n','')
          csvFile.write(csvTxt + "\n")
          poi = poidata.decode().split(',', -1)
        # print(hKeys)
    csvFile.close()

  def getSchema(self, slKey):
    slSchema = pickle.loads(self.r.get(slKey + "schema"))
    return (slSchema)

  # def getTile(self,tKey):

  # restore part

  def restore(self, appendMode=False):
    indexJsPath = self.outputPath + "index.json"
    indexJsFile = open(indexJsPath, mode="r", encoding="utf-8")
    indexJson = json.load(indexJsFile)
    # print ( indexJson )
    if not appendMode:
      print("Restore Mode: remove All Data")
      self.r.flushdb()  # DBを消去する 全部消してしまう(ns関係なく)
      # csv2redis.deleteAllData()
    else:
      print("Append mode")

    for dbNS in indexJson:
      print(dbNS, indexJson[dbNS])
      schemaJsPath = self.outputPath + dbNS + ".json"
      schemaJsFile = open(schemaJsPath, mode="r", encoding="utf-8")
      schemaJson = json.load(schemaJsFile)
      # print(schemaJson)
      self.registSchema(schemaJson)

      csvDataPath = self.outputPath + dbNS + ".csv"
      self.registCsvData(csvDataPath, schemaJson)

  def registCsvData(self, csvPath, schema):
    file = self.csv2redis.getCsvReader(csvPath)
    print(schema)
    self.csv2redis.readAndRegistData(file, schema["latCol"], schema["lngCol"], self.csv2redis.maxLevel)
    self.csv2redis.buildAllLowResMap()

  def registSchema(self, schemaJsonData):
    ans = self.csv2redis.registSchema(schemaJsonData)
    if (ans == True):
      print("Regist:", schemaJsonData["namespace"])
      return ("OK")
    else:
      print("DUPLICATED:", schemaJsonData["namespace"])
      return ("DUPLICATED ERROR")
    return ans


# END OF CLASS


def main(restoreMode, dumpDir, appendMode):
  dump = Csv2redisDumperClass(redisDBNumber, dumpDir)
  #ds = dump.getDataSet()
  # print(json.dumps(dump.decode_dict(ds)))
  if (restoreMode):
    dump.restore(appendMode)
  else:
    dump.listSubLayers()


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--dbnumb', default='0')
  parser.add_argument("--restore", action='store_true')  # レストアモード
  parser.add_argument("--append", action='store_true')  # 追加モード
  parser.add_argument('--dir', default='./dump')  # ダンプディレクトリ指定

  args = parser.parse_args()
  # いずれもない場合はバックアップモード(データの保存)
  dumpDir = "./dump"
  if os.path.isdir(args.dir):
    dumpDir = os.path.normpath(args.dir)
  else:
    print("ディレクトリ指定誤っています")

  print("target directory:", dumpDir)

  print('dbnumb : ', args.dbnumb)
  redisDBNumber = int(args.dbnumb)
  if (args.restore):
    main(True, dumpDir, args.append)
  else:
    main(False, dumpDir, args.append)
