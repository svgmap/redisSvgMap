# Redis QuadTreeCompositeSVGMapシステムを、Flaskの完全に動的なサーバとして稼働させる試行
# Programmed by Satoru Takagi
# 2019/02/07 The 1st revision: すでに、小縮尺をベクタにするか、ラスタにするかが選べる実装になってるよ～
# 2019/04/05 大量情報一括登録機能を実装
# 2019/04/xx 登録受付を即座にリプライして、登録状態を提供できるようにする(ただし登録はシングルタスクに制限)
# 2019/04/25 rev3: use csv2redis13 DB namespace
# 2019/05/13 ついにサブレイヤー機能が大枠で完成！

#Lesson0
# https://www.pytry3g.com/entry/Flask-Quickstart
#Lesson1
# https://auth0.com/blog/jp-developing-restful-apis-with-python-and-flask/
# Print log to console: https://stackoverflow.com/questions/5574702/how-to-print-to-stderr-in-python

# python import ディレクトリ指定: https://note.nkmk.me/python-relative-import/, http://d.hatena.ne.jp/chlere/20110618/1308369842 , http://python.g.hatena.ne.jp/edvakf/20090424/1240521319

# Query part get: https://stackoverflow.com/questions/11774265/how-do-you-get-a-query-string-on-flask
# サブパスを拾う　：http://takeshid.hatenadiary.jp/entry/2015/12/15/23395
# 動的画像出力(tempファイルなしで): https://stackoverflow.com/questions/7877282/how-to-send-image-generated-by-pil-to-browser ただし、py3ではhttp://naoyashiga.hatenablog.com/entry/2016/05/21/182942
# テンプレートを使う:https://www.sejuku.net/blog/55507
# https://qiita.com/redshoga/items/60db7285a573a5e87eb6 単にファイルを返すとか

from flask import Flask, request, send_from_directory, Response, send_file
from flask_cors import CORS
import urllib.parse
import sys, os
import json
import re
import math
from collections import OrderedDict
# import pprint
import threading

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from csv2redis16 import Csv2redisClass  # 上で上のディレクトリをappendしてるのでimportできる

app = Flask(__name__)
CORS(app)

now_registering = False  # POIの登録中を示すフラグ（登録をシングルに制限）

generalHkey = True  # 個々のデータを投入するhmapのkey(これはgeoHashではなく、その下の個々のデータを入れる入れ物のハッシュキー(なのでどうでもいいといえばどうでもいい))をどうするか

SAVE_DIR = "webApps"

# LowResImage = False  # 小縮尺タイルをrectVectorではなくPNGビットイメージにする場合はTrueに
LowResImage = False

dbnsDefault = "s2_"  # use csv2redis13 for DB namespace
#dbns = ""


@app.route("/")
def hello_world():
  return "Hello, World!<br><a href='svgmap/'>to svgmap redis</a>"


@app.route("/query")
def hello_world2():
  ustr = (request.query_string).decode()
  user = request.args.get('hello')
  print('This is error output', file=sys.stderr)
  return ("QUERY:" + urllib.parse.unquote(ustr) + " : " + urllib.parse.unquote(user))


defauleVariable = "000"
defauleVariable2 = "V"


@app.route("/dataset/<variable>/show")
@app.route("/dataset/show")
@app.route("/dataset/<variable>/show/<variable2>")
def hello_world3(variable=defauleVariable, variable2=defauleVariable2):
  return ("variable:" + variable + " variable2:" + variable2)


redisRegistJob = {}  # ここに下のインスタンスが一個だけ作られる感じ


class redisRegistThread(threading.Thread):

  def __init__(self, dsHash):
    super(redisRegistThread, self).__init__()
    self.stop_event = threading.Event()
    self.progress = "INIT"
    self.jsStr = ""
    self.dbns = dsHash
    self.csv2redis = Csv2redisClass()
    self.csv2redis.init(self.dbns)
    self.latCol = self.csv2redis.schemaObj.get("latCol")
    self.lngCol = self.csv2redis.schemaObj.get("lngCol")

  def stop(self):
    self.stop_event.set()

  def run(self):
    global allLowResMapLen
    self.progress = "S0_jsonLoading"
    jsData = json.loads(self.jsStr)
    # print("json:", jsData, file=sys.stderr)
    geoHashes = set()
    #    self.csv2redis.init(self.dbns)
    if jsData["action"] == "MODIFY":
      print("MODIFY: start : dbNamespace:", self.dbns, file=sys.stderr)
      originPois = getData(jsData["from"], self.latCol, self.lngCol)
      changeToPois = getData(jsData["to"], self.latCol, self.lngCol)
      count = 0
      for originPoi in originPois:
        ret = self.csv2redis.deleteData((originPoi), self.csv2redis.maxLevel)
        geoHashes.update(ret["keys"])
        self.progress = "S1_" + str(count) + "/" + str(len(originPois))
        count = count + 1
      ret = self.csv2redis.flushDeleteData(self.csv2redis.maxLevel)
      geoHashes.update(ret["keys"])
      print("Step1 DELETE: end : geoHashes:", geoHashes, file=sys.stderr)
      count = 0
      for changeToPoi in changeToPois:
        ret = self.csv2redis.registData((changeToPoi), self.csv2redis.maxLevel)
        geoHashes.update(ret["keys"])
        self.progress = "S2_" + str(count) + "/" + str(len(changeToPois))
        count = count + 1
      ret = self.csv2redis.flushRegistData(self.csv2redis.maxLevel)
      geoHashes.update(ret["keys"])
      print("Step2 ADD: end : geoHashes:", geoHashes, file=sys.stderr)

    elif jsData["action"] == "ADD":
      addPois = getData(jsData["to"], self.latCol, self.lngCol)
      print("ADD: start : dbns: ", self.dbns, file=sys.stderr)
      count = 0
      for addPoi in addPois:
        ret = self.csv2redis.registData((addPoi), self.csv2redis.maxLevel)
        geoHashes.update(ret["keys"])
        self.progress = "S1_" + str(count) + "/" + str(len(addPois))
        count = count + 1
      ret = self.csv2redis.flushRegistData(self.csv2redis.maxLevel)
      print("registKeys:", ret)
      geoHashes.update(ret["keys"])
      print("ADD: update lrmap")

    elif jsData["action"] == "DELETE":
      delPois = getData(jsData["from"], self.latCol, self.lngCol)
      print("DELETE: start : dbns: ", self.dbns, delPois, file=sys.stderr)
      count = 0
      for delPoi in delPois:
        ret = self.csv2redis.deleteData((delPoi), self.csv2redis.maxLevel)
        geoHashes.update(ret["keys"])
        self.progress = "S1_" + str(count) + "/" + str(len(delPois))
        count = count + 1
      ret = self.csv2redis.flushDeleteData(self.csv2redis.maxLevel)
      geoHashes.update(ret["keys"])
      print("DELETE: update lrmap")
      print("DELETE: end : geoHashes:", geoHashes, file=sys.stderr)
    self.progress = "S3_"
    allLowResMapLen = len(geoHashes)
    self.csv2redis.updateLowResMaps(geoHashes)
    print("COMPLETED : geoHashes:", geoHashes, file=sys.stderr)
    self.progress = "COMPLETED"


@app.route("/svgmap/<dsHash>/editPoint", methods=['POST'])
@app.route("/svgmap/editPoint", methods=['POST'])
def capturePost(dsHash=dbnsDefault):
  global redisRegistJob
  if (isinstance(redisRegistJob, redisRegistThread) and redisRegistJob.isAlive()):
    return ("Now registering.. Retry layer.")

  # print(request.headers, file=sys.stderr)
  jsStr = (request.data).decode()
  # print(jsStr, file=sys.stderr)
  redisRegistJob = redisRegistThread(dsHash)
  redisRegistJob.jsStr = jsStr
  redisRegistJob.start()
  return "POST Accepted."


allLowResMapLen = 0


@app.route("/svgmap/<dsHash>/editStatus")
@app.route("/svgmap/editStatus")
def getEditStat(dsHash=dbnsDefault):
  # ISSUE dsHashを弁別していない
  if (isinstance(redisRegistJob, redisRegistThread) and redisRegistJob.isAlive()):
    regStat = redisRegistJob.progress
    if regStat == "S3_":
      regStat = "S3_" + str(redisRegistJob.csv2redis.getBuildAllLowResMapCount()) + "/" + str(allLowResMapLen)
      pass
    return ("Status: " + regStat)
  else:
    return ("Not registering")


@app.route("/svgmap/listSubLayers")
def listSubLasyers():
  csv2redis = Csv2redisClass()
  sl = csv2redis.listSubLayers()
  print(sl)
  dsl = {}
  for key in sl:
    dsl[key.decode()] = (sl.get(key)).decode()

  ans = json.dumps(dsl)
  print(ans)
  # return ans
  return Response(ans, mimetype="application/json")


@app.route("/svgmap/buildLayer", methods=['POST'])
def buildLayer():
  jsStr = (request.data).decode()
  jsonData = json.loads(jsStr)
  print("called buildLayer: parsedJson: ", jsonData)
  csv2redis = Csv2redisClass()
  ans = csv2redis.registSchema(jsonData)
  if (ans == True):
    return ("OK")
  else:
    return ("DUPLICATED ERROR")


def toNumber(coord):
  if type(coord == str):
    return float(coord)
  else:
    return coord


reg = re.compile(r"[^,]")


def getData(poiDatas, latCol, lngCol):
  # redisに投入するPOIデータを生成する(csvではなくWebSertvice(Flask)で投入するとき用。csvファイル(バッチ)要は、csv2redisの中のgetOneData()が相当)
  out = []
  for poiData in poiDatas:
    lat = toNumber(poiData["latitude"])
    lng = toNumber(poiData["longitude"])
    meta = poiData["metadata"]

    if (generalHkey == True):
      hkey = str(math.floor(lat * 100000)) + ":" + str(math.floor(lng * 100000)) + ":" + meta
      # POIのIDは苦慮中・・・ひとまずほとんど何も考えないタイプでやってみる・・・(全部のデータを（ただし緯度経度は小数点以下５桁で丸め)足し合した文字列でキーとする））
      print("generalHkey:", hkey)
    else:
      if (reg.search(meta)):
        # POIのIDは苦慮中・・・metaにカンマ以外の文字があったらmetaをID(POI識別用HashKey)とする 2019/4/2
        hkey = meta
      else:
        # metaがカンマ以外何もないときは緯度経度の100000倍の値をHashKeyにする
        hkey = str(math.floor(lat * 100000)) + ":" + str(math.floor(lng * 100000))

    # "data"の中身がredisの実データとして投入される
    if (latCol == 0 and lngCol == 1):
      oneData = {"lat": lat, "lng": lng, "data": str(lat) + "," + str(lng) + "," + meta, "hkey": hkey}
    else:
      splitMeta = meta.split(',', -1)
      if (latCol > lngCol):
        splitMeta.insert(lngCol, str(poiData["longitude"]))
        splitMeta.insert(latCol, str(poiData["latitude"]))
      else:
        splitMeta.insert(latCol, str(poiData["latitude"]))
        splitMeta.insert(lngCol, str(poiData["longitude"]))
      oneData = {"lat": lat, "lng": lng, "data": ",".join(splitMeta), "hkey": hkey}

    out.append(oneData)
  return out


@app.route("/svgmap/deleteAllData")
@app.route("/svgmap/<dsHash>/deleteAllData")
def deleteAllData(dsHash=dbnsDefault):
  if checkLock():
    print("Get delete all data command")
    csv2redis = Csv2redisClass()
    csv2redis.init(dsHash)
    dc = csv2redis.deleteAllData()
    clearLock()
    return ("OK deletedGeoHashCount:" + str(dc))
  else:
    return ("Please retry. Now registering.")


@app.route("/svgmap/removeDataset/<dsHash>")
def removeDataset(dsHash):
  if checkLock():
    print("Get removeDataset command")
    csv2redis = Csv2redisClass()
    csv2redis.init(dsHash)
    dc = csv2redis.deleteAllData(True)
    clearLock()
    return ("OK removeDataset:" + str(dc))
  else:
    return ("Please retry. Now registering.")


def checkLock():
  global now_registering
  if now_registering == False:
    now_registering = True
    return True
  else:
    return False


def clearLock():
  global now_registering
  now_registering = False


@app.route("/svgmap/<dsHash>/<tileName>")
@app.route("/svgmap")
@app.route("/svgmap/")
@app.route("/svgmap/<tileName>")
def getMalTile(tileName="index.html", dsHash=dbnsDefault):

  print("Req. tileName:" + tileName, file=sys.stderr)

  if tileName == "":
    tileName = "index.html"

  if tileName.startswith("svgMapTile") and (tileName.endswith(".svg") or tileName.endswith(".png")):
    geoHash = None
    spos = 10
    epos = tileName.find(".")
    if (spos < 0 or epos < 0):
      geoHash = "D"
    else:
      geoHash = tileName[spos:epos]

    # print("parsed:" + str(spos) + "," + str(epos) + " name:" + tileName + "  geoHash:" + geoHash, file=sys.stderr)

    #    print("parsed:" + tileName[spos, epos], file=sys.stderr)

    # print("tile Numb:" + str(geoHash), file=sys.stderr)
    # print("tileName:" + tileName, file=sys.stderr)

    csv2redis = Csv2redisClass()

    csv2redis.init(dsHash)
    # print(csv2redis.csvSchema, file=sys.stderr)
    # print(csv2redis.csvSchemaType, file=sys.stderr)
    if geoHash == None:
      geoHash = "D"
    if tileName.endswith(".svg"):
      svgContent = csv2redis.saveSvgMapTileN(geoHash, None, LowResImage, True)
      return Response(svgContent, mimetype='image/svg+xml')
    else:  # PNG
      pngByteIo = csv2redis.saveSvgMapTileN(geoHash, None, LowResImage, True, True)
      return send_file(pngByteIo, mimetype='image/png')
  elif tileName.startswith("svgMap") and tileName.endswith(".svg"):  # for root svg content
    csv2redis = Csv2redisClass()
    csv2redis.init(dsHash)
    svgContent = csv2redis.saveSvgMapTileN(None, None, LowResImage, True)
    return Response(svgContent, mimetype='image/svg+xml')
  else:  # それ以外の場合は指定ディレクトリの静的ファイルを送る
    return send_from_directory(SAVE_DIR, tileName)


if __name__ == "__main__":
  app.run()
