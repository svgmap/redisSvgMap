# Redis QuadTreeCompositeSVGMapシステムを、Flaskの完全に動的なサーバとして稼働させる試行
# Programmed by Satoru Takagi
# 2019/2/7 The 1st revision: すでに、小縮尺をベクタにするか、ラスタにするかが選べる実装になってるよ～

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
import urllib.parse
import sys, os
import json
import re
import math
from collections import OrderedDict
# import pprint

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import csv2redis12 as csv2redis  # 上で上のディレクトリをappendしてるのでimportできる

app = Flask(__name__)

SAVE_DIR = "webApps"

# LowResImage = False  # 小縮尺タイルをrectVectorではなくPNGビットイメージにする場合はTrueに
LowResImage = False


@app.route("/")
def hello_world():
  return "Hello, World!"


@app.route("/query")
def hello_world2():
  ustr = (request.query_string).decode()
  user = request.args.get('hello')
  print('This is error output', file=sys.stderr)
  return ("QUERY:" + urllib.parse.unquote(ustr) + " : " + urllib.parse.unquote(user))


@app.route("/svgmap/editPoint", methods=['POST'])
def capturePost():
  # print(request.headers, file=sys.stderr)
  jsStr = (request.data).decode()
  # print(jsStr, file=sys.stderr)
  jsData = json.loads(jsStr)
  print("json:", jsData, file=sys.stderr)
  geoHashes = []
  csv2redis.init()
  if jsData["action"] == "MODIFY":
    print("MODIFY: start : ", file=sys.stderr)
    originPois = getData(jsData["from"])
    changeToPois = getData(jsData["to"])
    for originPoi in originPois:
      ret = csv2redis.deleteData((originPoi), csv2redis.maxLevel)
      geoHashes.extend(ret["keys"])
    ret = csv2redis.flushDeleteData(csv2redis.maxLevel)
    geoHashes.extend(ret["keys"])
    print("Step1 DELETE: end : geoHashes:", geoHashes, file=sys.stderr)

    for changeToPoi in changeToPois:
      ret = csv2redis.registData((changeToPoi), csv2redis.maxLevel)
      geoHashes.extend(ret["keys"])
    ret = csv2redis.flushRegistData(csv2redis.maxLevel)
    geoHashes.extend(ret["keys"])
    print("Step2 ADD: end : geoHashes:", geoHashes, file=sys.stderr)

    csv2redis.updateLowResMaps(set(geoHashes))
    print("MODIFY: end : ", file=sys.stderr)

  elif jsData["action"] == "ADD":
    addPois = getData(jsData["to"])
    print("ADD: start : ", addPois, file=sys.stderr)
    for addPoi in addPois:
      ret = csv2redis.registData((addPoi), csv2redis.maxLevel)
      geoHashes.extend(ret["keys"])
    ret = csv2redis.flushRegistData(csv2redis.maxLevel)
    print("registKeys:", ret)
    geoHashes.extend(ret["keys"])
    print("ADD: update lrmap")
    csv2redis.updateLowResMaps(set(geoHashes))
    print("ADD: end : geoHashes:", geoHashes, file=sys.stderr)

  elif jsData["action"] == "DELETE":
    delPois = getData(jsData["from"])
    print("DELETE: start : ", delPois, file=sys.stderr)
    for delPoi in delPois:
      ret = csv2redis.deleteData((delPoi), csv2redis.maxLevel)
      geoHashes.extend(ret["keys"])
    ret = csv2redis.flushDeleteData(csv2redis.maxLevel)
    geoHashes.extend(ret["keys"])
    print("DELETE: update lrmap")
    csv2redis.updateLowResMaps(set(geoHashes))
    print("DELETE: end : geoHashes:", geoHashes, file=sys.stderr)

  return request.data


def toNumber(coord):
  if type(coord == str):
    return float(coord)
  else:
    return coord


reg = re.compile(r"[^,]")


def getData(poiDatas):
  out = []
  for poiData in poiDatas:
    lat = toNumber(poiData["latitude"])
    lng = toNumber(poiData["longitude"])
    meta = poiData["metadata"]
    if (reg.search(meta)):
      # POIのIDは苦慮中・・・metaにカンマ以外の文字があったらmetaをID(POI識別用HashKey)とする 2019/4/2
      hkey = meta
    else:
      # metaがカンマ以外何もないときは緯度経度の100000倍の値をHashKeyにする
      hkey = str(math.floor(lat * 100000)) + ":" + str(math.floor(lng * 100000))

    oneData = {"lat": lat, "lng": lng, "data": str(lat) + "," + str(lng) + "," + meta, "hkey": hkey}

    out.append(oneData)
  return out


@app.route("/svgmap/deleteAllData")
def deleteAllData():
  print("Get delete all data command")
  csv2redis.init()
  dc = csv2redis.deleteAllData()
  return ("OK deletedGeoHashCount:" + str(dc))


@app.route("/svgmap")
@app.route("/svgmap/")  # これもいるの？？
@app.route("/svgmap/<tileName>")
def getMalTile(tileName="index.html"):

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

    csv2redis.init()
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
    csv2redis.init()
    svgContent = csv2redis.saveSvgMapTileN(None, None, LowResImage, True)
    return Response(svgContent, mimetype='image/svg+xml')
  else:
    return send_from_directory(SAVE_DIR, tileName)


if __name__ == "__main__":
  app.run()
