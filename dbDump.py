# dbDump.py
# SVGMap/redisのデータベースの内容を、オリジナルの再利用しやすい形でファイル保存する。
# Programmed by Satoru Takagi
# History:
# 2023/02/08 1st release

import redis
import json
import pickle
import argparse

# import codecs


class Csv2redisDumperClass():
  def __init__(self, dbNumb=0, outputPath="./dump/"):
    self.r = redis.Redis(host='localhost', port=6379, db=dbNumb)
    self.outputPath = outputPath

  def getDataSet(self):
    ds = self.r.hgetall("dataSet")
    return (ds)

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
    sl = self.getDataSet()
    # print("SubLayers:",sl)
    indexJsPath = self.outputPath + "index.json"
    indexJsFile = open(indexJsPath, mode="w", encoding="utf-8")
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
    schemaFile = open(schemaPath, mode="w", encoding="utf-8")
    json.dump(slSchema, schemaFile, indent=2, ensure_ascii=False)
    schemaFile.close()

    csvPath = self.outputPath + slKey + ".csv"
    csvFile = open(csvPath, mode="w", encoding="utf-8")
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
          csvFile.write(poidata.decode() + "\n")
          poi = poidata.decode().split(',', -1)
        # print(hKeys)
    csvFile.close()

  def getSchema(self, slKey):
    slSchema = pickle.loads(self.r.get(slKey + "schema"))
    return (slSchema)

  # def getTile(self,tKey):


# END OF CLASS


def main():
  dump = Csv2redisDumperClass(redisDBNumber)
  #ds = dump.getDataSet()
  # print(json.dumps(dump.decode_dict(ds)))
  dump.listSubLayers()


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--dbnumb', default='0')
  args = parser.parse_args()
  print('dbnumb : ', args.dbnumb)
  redisDBNumber = int(args.dbnumb)
  main()
