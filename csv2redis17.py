import sys
from io import BytesIO
import csv
import codecs
from xml.dom import minidom
import argparse
import redis
import pickle
#import csv2svgmap
from PIL import Image
import math
import numpy as np
import hashlib
from lib.svgmapContainer import SvgmapContainer, Tag

# csvを読み込み、redis上に、quadtreeのタイル番号をハッシュキーとした非等分quadtreeデータ構造を構築する
# Programmed by Satoru Takagi
# Copyright 2018 by Satoru Takagi all Rights Reserved
# License GPL Ver.3
#
# History:
#  Rev1  2018/11/06
#  Rev4  2018/11/12 Quad Tree Div
#  Rev5  2018/11/22 LowResMap新アルゴリズムの実装開始
#  Rev6  2019/01/09 最初の動作版完成 (バッチ処理タイプ)
#  Rev7  2019/01/10 動的サーバに向けた改修
#  Rev8  2019/01/11 バースト更新における高効率化
#  Rev9  2019/01/16 高速化のためSVGMapコンテンツをXMLでなく単なるtextとして生成する変更
#  Rev10 2019/02/01 Low Resをビットイメージで出力
#        2019/02/07 Flask等による動的サーバとして動かせるようなライブラリ化を達成！ helloFlask.pyのほうがメインルーチンになる
#        2019/02/19 効率化(LowResImageのときビットイメージコンテナ生成時に低解像データ読まないように)
#  Rev11 2019/02/21 hashとして保存し、このHashCodeをベースとしたデータの削除＆QuadTreeCompositeTiligの再構築を可能にする
#        2019/02/26 tileABCDを統合するルートSVGを作れるようにした。　その他マイナーバグフィックス
#  Rev12  非同期処理に対応させる　コールバックによって登録の進捗具合を返信できるようにする予定　これによって膨大なデータの登録をWebI/Fで可能にするつもり
#  Rev13  2019/04/25 (MainKeyの)ネームスペースを実装して、複数のPOIデータセット(レイヤー)を、(redisのDB数に依存せず)ひとつのRedisDBの中で扱えるようにする。
#  Rev14  2019/05/xx Issue #2,#3対応(Schemaおよび登録データの整理)
#  Rev15  ちょっと飛ばしてます(svg出力ルーチンカスタマイザブル) Rev17で再度
#  Rev16  2019/08/02 クラス化しました
#  Rev17  2019/08/23 Rev14のpull req.内容をRev16に適用しました
#         2019/08/23 完全カスタムsvg出力ルーチンフレームワークを入れはじめた（customDataGenerator、customLowResGenerator）
#
# How to use redis...
# https://redis-py.readthedocs.io/en/latest/
# http://www.denzow.me/entry/2017/10/07/233233
# http://www.denzow.me/entry/2017/10/08/212059
#
# To Do
#  DataそものもをRedisの実データタイルのHashTableのHashKeyにしちゃってるのを、レコード番号とかID番号とか特定のカラムをハッシュキーに指定できる機能はやっぱあるべき
#  個々のPOIをIDをベースに更新したり削除したりする機能
#  小縮尺データをビットイメージで出力する機能
#  オンデマンドでコンテンツを生成するwebサーバ機能
#
# ISSUES
#  DONE: 動的出力で小縮尺ビットイメージモードの場合は、もっと効率化できる（ビットイメージ生成はSVGファイル要求時は不要なので）
#  registDataのバリデーションチェックがされてない（カラム数が違っててもどんどん登録してしまう・・・）


class Csv2redisClass():

  def __init__(self):
    #global 変数たち
    #self.r
    self.r = redis.Redis(host='localhost', port=6379, db=0)
    self.listLimit = 500
    self.lowresMapSize = 128  # 集約データの分解能の定義(この数値のスクエア)　この実装では2の倍数である必要がある(上の階層の倍という意味で)
    #self.lowResIsBitImage = True

    self.schemaObj = {
        "schema": [],
        "type": [],
        "latCol": -1,
        "lngCol": -1,
        "titleCol": -1,
        "idCol": -1
    }  # 以下の個別の変数をオブジェクト化してまとめ 2019/5/7
    # schema, type, latCol, lngCol, titleCol, idCol

    # 以下はschemaObjにまとめ
    # csvSchema = []
    # csvSchemaType = []
    # latCol = -1
    # lngCol = -1
    # titleCol = -1
    # idCol = -1  # 2019/4/2 geoHash下のhashSetのhashKeyとして、このカラムを使う（なければ適当に作っている・・・ メタデータあればメタデータ全部足した文字列、なければ緯度経度の100000倍をつなげたもの

    self.overFlowKeys = {}  # quadPartを実行し、子供の階層を構築したメッシュ （オンメモリであり永続化されているわけではない）
    self.updatedLrMapKeys = {
    }  # 上のdictで足りると思うが、テストのために実装してみる　pointを追加したとき、そのpointを直上のlrMapのdictを入れる　今後恐らく一点単位の登録が発生すると意味が出てくる？

    self.maxLevel = 16  # この値をもとにタイリングは行われない・・・・ほとんど意味ない状態(2019.8.2)
    self.targetDir = "mapContents"  # コンテンツ出力ディレクトリ
    self.ns = ""
    self.ns = ""  # This is nameSpace str
    self.registDataList = []
    self.burstSize = 600
    self.buildAllLowResMapCount = 0
    self.deleteDataList = []

    # 色(アイコン種)分けに関するオプション用：デフォルトはいつも同じビットイメージアイコンを使う感じ 2019.8.23 s.takagi
    self.poi_color = [{"flag": "f1", "color": "mappin.png"}]
    self.poi_index = None  # アイコン種を変化させるためのメタデータの番号(lat,lng除く)
    self.poi_size = ["-8", "-25", "19", "27"]  # x,y,width,height

    # 完全にカスタムなコンテンツ生成ルーチンを組み込むI/F
    # customDataGeneratorオブジェクトは、outputSvgContent([poiObj],svgc:SvgmapContainer):str(xmlStr)を実装している必要がある
    # poiObj(Dict) = {"lat", "lng", "title", "metadata"}
    #    svgcにはヘッダやCRSが設定されています
    self.customDataGenerator = None
    # customLowResGeneratorも同様　(TBD)
    self.customLowResGenerator = None

  # static for schema["Type"]
  T_ENUM = 0
  T_NUMB = 1
  T_STR = 2
  scehmaTypeStr = ["enumeration", "number", "string"]

  svgFileNameHd = "svgMapTile"
  topVisibleMinZoom = 4.5

  UseRedisHash = True  # これはもはやTrue固定です　あとでFalseケースの実装を外します 2019/3/13

  def registLrMap(self, lrMap, xyKey, splitedData):
    # 低解像度統計データを構築・登録する
    # splitedData: 登録しようとしているデータ
    # xyKey: そのデータの下記lrMapにおけるピクセル座標を用いたハッシュキー
    # lrMap{}のデータ構造：
    # そのズームレベルのタイルにおける、ラスターカバレッジ
    # ラスターの解像度はx:lowresMapSize x y:lowresMapSize
    # lrMapのKeyは、x_y = xyKey : ラスターピクセル座標のハッシュ
    # lrMap["total"](total特殊キー)には総ポイント個数
    # 他のxyKeyのlrMapのValueは、そのデータのプロパティに対応する配列
    # [0..len(csvSchema)-1]以下のデータが入る
    #    メタデータがString(T_STR)の場合：文字数の合計：あまり意味がないが・・
    #    メタデータがEnum(T_ENUM)の場合：HashMap{KK,VV}
    #      KK: メタデータの値(Str)
    #      VV: その値を持つ個数
    #    メタデータがNumber(T_NUMB)の場合：合計値[0]と有効数[1]
    # [len(csvSchema)]
    #   そのピクセルにおけるPOI総個数
    if ("total" in lrMap):
      lrMap["total"] += 1
    else:
      lrMap["total"] = 1

    csvSchemaType = self.schemaObj.get("type")

    if (xyKey in lrMap):
      pixData = lrMap[xyKey]
      pixData[len(csvSchemaType)] += 1
    else:
      pixData = [None] * (len(csvSchemaType) + 1)  # None(null)が入ったlen(csvSchemaType)+1個のListを作る
      pixData[len(csvSchemaType)] = 1  # 個数のフィールド(注意)
      lrMap[xyKey] = pixData

    #pixData: lrMap中の１ピクセル分のメタデータ統計情報
    #pixData[i]: i番目のメタデータに関する統計情報
    for i, val in enumerate(splitedData):
      # print (i,"::",val)
      if (val != ""):
        if csvSchemaType[i] == Csv2redisClass.T_ENUM:  # 列挙型データの統計
          if pixData[i] is None:
            pixData[i] = {}
          if (val in pixData[i]):
            pixData[i][val] += 1
          else:
            pixData[i][val] = 1
        elif csvSchemaType[i] == Csv2redisClass.T_NUMB:  # 数値型データの統計　最大最小・標準偏差とかもあると良いと思うが・・
          if (val != "-"):
            fVal = float(val)
            if (pixData[i] is None):
              pixData[i] = []
              pixData[i].append(fVal)
              pixData[i].append(1)
              pixData[i].append(fVal)  # minVal to pixData[i][2] added 2019/4/11
              pixData[i].append(fVal)  # maxVal to pixData[i][3]
            else:
              pixData[i][0] += fVal
              pixData[i][1] += 1
              if (pixData[i][2] > fVal):  # minVal added 2019/4/11
                pixData[i][2] = fVal
              elif (pixData[i][3] < fVal):  # maxVal
                pixData[i][3] = fVal
        else:  # 文字列型　あまり意味ない　とりあえず総文字数カウントでもしとくか・・
          if (pixData[i] is None):
            pixData[i] = len(val)
          else:
            pixData[i] += len(val)

  def addLowResMap(self, targetGeoHash, lat, lng, poidata, lrMap, lat0, lng0, lats, lngs):
    # poidata：split済みのメタデータ文字列　ただし０，１番目に緯度経度入り
    # lat0, lng0, lats, lngs = geoHashToLatLng(targetGeoHash)
    # print("\nbuildLowResMap: targetLatLng:", lat0, lng0, lats, lngs)
    if lat >= 90.0:
      lat = 89.9999999
    if lng >= 180.0:
      lng = 179.9999999

    lati = int(self.lowresMapSize * (lat - lat0) / lats)
    lngi = int(self.lowresMapSize * (lng - lng0) / lngs)
    lrPixKey = str(lngi) + "_" + str(lati)  # x_y : 経度方向のピクセル_緯度方向のピクセル
    if (lati >= self.lowresMapSize or lngi >= self.lowresMapSize):
      print(lati, lngi, lat, lng, lats, lngs, lat0, lng0, file=sys.stderr)
      raise NameError('outOfRangeErr')
    # print("addLowResMap", lrPixKey)
    self.registLrMap(lrMap, lrPixKey, poidata)

  def geoHashToLatLng(self, hash):
    char_list = list(hash)
    lats = 180
    lngs = 180
    lat = -180
    lng = -180
    for oneChar in char_list:
      if oneChar == "A":
        lat += 0  # do nothing...
        lng += 0
      elif oneChar == "B":
        lat += 0
        lng += lngs
      elif oneChar == "C":
        lat += lats
        lng += 0
      else:
        lat += lats
        lng += lngs
      lats = lats / 2
      lngs = lngs / 2

  #  print(hash,lat,lng,lats,lngs)
    return lat, lng, 2 * lats, 2 * lngs

  def getGeoHashCode(self, lat, lng, lat0, lng0, lats, lngs):
    la = 0
    lngs = lngs / 2
    if lng < (lng0 + lngs):
      la = 0
    else:
      la = 1
      lng0 = lng0 + lngs

    lats = lats / 2
    if lat < (lat0 + lats):
      la += 0
    else:
      la += 2
      lat0 = lat0 + lats
    ans = chr(65 + la)  # ABCD  lng:A->B  lat:A->C
    return ans, lat0, lng0, lats, lngs

  def quadPart(self, ans, lat0, lng0, lats, lngs, latCol, lngCol):
    # 登録個数の上限に達したタイルのデータを４分割
    # 分割後にLowResMapを構築するのは、一連の全データの追加後に行うこととしてみる
    # そのかわり、データ追加に伴って影響を受けたLowResmapのhashをオンメモリに貯めることにしてみる（これはこの関数内ではなく、redisに個々のデータを追加している段階で行うこと）
    #    global r, overFlowKeys
    if Csv2redisClass.UseRedisHash:
      src = list((self.r.hgetall(self.ns + ans)).values())
      hKeys = list((self.r.hgetall(self.ns + ans)).keys())
    else:
      src = self.r.lrange(self.ns + ans, 0, -1)  # 分割元のデータ

    #以下pipelineを使うと早くなるかな
    pipe = self.r.pipeline()
    # lowResMap = {}  # ここに、最大lowresMapSize^2のhashKey数で低解像度用のメッシュ型カバレッジデータ構造が構築される
    count = 0
    for poidata in src:
      #    print ("quadPart",data.decode())
      poi = poidata.decode().split(',', -1)
      lat = float(poi[latCol])
      lng = float(poi[lngCol])
      # addLowResMap(ans, lat, lng, poi, lowResMap, lat0, lng0, lats, lngs) # このタイミングで行う必要はないと思う　一連のデータ登録が完了したタイミングで行うのが良いと思う。
      ans0, latN0, lngN0, latNs, lngNs = self.getGeoHashCode(lat, lng, lat0, lng0, lats, lngs)
      key = ans + ans0
      if Csv2redisClass.UseRedisHash:
        hkey = hKeys[count]
        pipe.hset(self.ns + key, hkey, poidata)
        count = count + 1
      else:
        pipe.rpush(self.ns + key, poidata)

    pipe.set(self.ns + ans, "OVERFLOW")
    pipe.execute()
    # storeLowResDataToRedis(r, ans, lowResMap) ということで、
    self.overFlowKeys[ans] = True
    print("\nEnd QuadPart:", ans, file=sys.stderr)

  def storeLowResDataToRedis(self, r, key, lowResMap):
    # use pickle http://www.denzow.me/entry/2017/10/08/212059
    # print("storeLowResDataToRedis", key, "  map:", lowResMap)
    self.r.set(self.ns + key, pickle.dumps(lowResMap))
    #ToDo: 上の階層に遡ってLowResを更新する必要がある
    #issue: この処理はredisアクセスおよびpickleパースが必要なのでpython上でキャッシュできると良いかも？
    # parentKey = key
    # childLowResMap = lowResMap

  def updateLowResMaps(self, keySet):
    allKeys = set()
    for key in keySet:
      allKeys |= (self.getAncestorsKeys(key))
    allKeys = sorted(allKeys, key=len)
    print("updateLowResMaps:", allKeys)
    self.buildAllLowResMap(allKeys)

  def getAncestorsKeys(self, key):
    ret = set()
    for i in range(len(key)):
      if (i == 0):
        sKey = key
      else:
        sKey = key[:-i]
      ret.add(sKey)
    return ret

  def updateAncestorsLowResMap(self, key):
    # 指定したLowResMap以上のものを更新する(そのポイントを子孫に持つものだけを生成しなおしなので少し早いと思う)
    # データ1個を追加した後に、そのデータの一つ上の階層のLowResMapを指定すれば、全LowResMapが更新したデータを反映したものになる
    print("updateAncestorsLowResMap", key, file=sys.stderr)
    for i in range(len(key)):
      if (i == 0):
        sKey = key
      else:
        sKey = key[:-i]  # もうちょっといいループの作り方あるでしょうね・・・

      # print("sKey:", sKey, self.r.type(sKey.encode()), file=sys.stderr)
      if (self.r.type((self.ns + sKey).encode()) == b"string"):
        # print("update LowResMap sKey", sKey, file=sys.stderr)
        thisTile = {}
        childTiles = []
        childTiles.append(self.getChildData(sKey + "A"))
        childTiles.append(self.getChildData(sKey + "B"))
        childTiles.append(self.getChildData(sKey + "C"))
        childTiles.append(self.getChildData(sKey + "D"))
        self.updateLowResMap(sKey, thisTile, childTiles)

  def getBuildAllLowResMapCount(self):
    print("getBuildAllLowResMapCount:", self.buildAllLowResMapCount)
    return self.buildAllLowResMapCount

  def buildAllLowResMap(self, keys=None):
    # 全LowResMapを一から生成しなおす（元のLowResMapデータがあっても利用せず上書き)
    #    global r, buildAllLowResMapCount
    if (keys is None):
      bkeys = self.r.keys(self.ns + "[A-D]*")
      bkeys.sort(key=len)  # 下のレベルのLowResMapから更新して上のレベルに波及させる必要があるのでgeohash長い順ソートする
      keys = []
      for key in bkeys:
        keys.append((key.decode())[len(self.ns):])

    # print ( "buildAllLowResMap: Keys:",bkeys )

    self.buildAllLowResMapCount = 0

    for key in reversed(keys):  # 下のレベルから
      # key = key.decode()
      if self.r.type(self.ns +
                     key) == b"string":  # そのタイルはオーバーフローしている実データがないタイル　実際にはpickleでバイナリ化したデータが入っているかb"OVERFLOW"がただ入ってる
        print(key, "STR", file=sys.stderr)
        thisTile = {}
        # thisTile = self.r.get(key)
        # if thisTile == b"OVERFLOW": # 更新ではなくて新規なのでこれは不要
        #  thisTile = {}
        #else:
        #  thisTile = pickle.loads(thisTile)

        childTiles = []
        childTiles.append(self.getChildData(key + "A"))
        childTiles.append(self.getChildData(key + "B"))
        childTiles.append(self.getChildData(key + "C"))
        childTiles.append(self.getChildData(key + "D"))

        self.updateLowResMap(key, thisTile, childTiles)

      else:  # そのタイルは実データが入っている(b"list")のデータ
        print("This is real data:", key, self.r.type(self.ns + key), file=sys.stderr)

      self.buildAllLowResMapCount = self.buildAllLowResMapCount + 1

  def printAllLowResMap(self):
    #    global r
    bkeys = self.r.keys(self.ns + "[A-D]*")
    bkeys.sort(key=len)
    print("printAllLowResMap: Keys:", bkeys, file=sys.stderr)

    for key in reversed(bkeys):
      key = key.decode()
      if self.r.type(self.ns +
                     key) == b"string":  # そのタイルはオーバーフローしている実データがないタイル　実際にはpickleでバイナリ化したデータが入っているかb"OVERFLOW"がただ入ってる
        thisTile = pickle.loads(self.r.get(self.ns + key))
        print("Key:", key, thisTile, file=sys.stderr)

  def getChildData(self, key):
    dType = self.r.type(self.ns + key)  # 高性能化のためexistsを排除
    if dType == b"string":  # overflowed lowResImage
      # print("child data is overflowtile ::: ", key)
      return (pickle.loads(self.r.get(self.ns + key)))
    elif dType == b"list":  # real data
      return (self.r.lrange(self.ns + key, 0, -1))
    elif dType == b"hash":  # real data by Hash 2019/2/19
      return (list((self.r.hgetall(self.ns + key)).values()))
    else:
      return (None)

  #    print(key, self.r.type(key))
  #    if ( self.r.type(key.decode()))
  #  for key in bkeys:
  #    keys.append(key.decode())
  #
  #  keys.sort(key=len)
  #  print (keys)

  def updateLowResMap(self, geoHash, lowResMap, childTiles):
    # その階層のLowResMapを更新する
    # この関数は再帰的に上に伸ばしていく処理の中で使われるはず 12/12
    # geoHash: そのlowResMapのgeoHashタイル番号
    # lowResMap: その階層のLowResMap
    # childTilsDatas: その階層の１段下の子供( 0..3 : A,B,C,D) lowResMapの場合もあるし、実データの場合もある
    # print("updateLowResMap : len:childTiles:", len(childTiles), file=sys.stderr)
    latCol = self.schemaObj.get("latCol")
    lngCol = self.schemaObj.get("lngCol")
    for i, childTile in enumerate(childTiles):
      if (childTile):
        if "total" in childTile:  # childもloweResMap
          px0 = 0
          py0 = 0
          if i == 1:
            px0 = self.lowresMapSize // 2
          elif i == 2:
            py0 = self.lowresMapSize // 2
          elif i == 3:
            px0 = self.lowresMapSize // 2
            py0 = self.lowresMapSize // 2
          self.updateLowResMapSub(lowResMap, childTile, px0, py0)
        else:  # childは実データ
          childGeoHash = geoHash
          if i == 0:
            childGeoHash += "A"
          elif i == 1:
            childGeoHash += "B"
          elif i == 2:
            childGeoHash += "C"
          else:
            childGeoHash += "D"
          lat0, lng0, lats, lngs = self.geoHashToLatLng(geoHash)  # debug geoHash: childじゃないのが正しいぞ 2019.1.8
          self.updateLowResMapData(lowResMap, childTile, lat0, lng0, lats, lngs, latCol, lngCol)

    print("updateLowResMap: set lowResData: ", geoHash, file=sys.stderr)
    if ("total" in lowResMap):
      self.r.set(self.ns + geoHash, pickle.dumps(lowResMap))
    else:
      print("tile:", geoHash, " is NULL. delete", file=sys.stderr)
      self.r.delete(self.ns + geoHash)

  def updateLowResMapData(self, parentLowResMap, childTile, lat0, lng0, lats, lngs, latCol, lngCol):
    # updateLowResMap用のサブルーチン
    # 子供が実データのときに、それを親に反映させる
    # registLrMap(addLowResMap) とほとんど同じルーチンなので共用化するべき
    for poidata in childTile:
      poi = poidata.decode().split(',', -1)  # decodeはredisから取ってきたデータなら必要だが・・
      lat = float(poi[latCol])
      lng = float(poi[lngCol])
      self.addLowResMap("", lat, lng, poi, parentLowResMap, lat0, lng0, lats, lngs)

  def updateLowResMapSub(self, parentLowResMap, childLowResMap, px0, py0):
    # updateLowResMap用のサブルーチン
    # 子供がlowResMapの時に、それを親に反映させる
    # 一つの子供のlowResMapを対象(親におけるその相対ピクセル位置px0,py0)
    # print("childLowResMap:",childLowResMap)
    csvSchemaType = self.schemaObj.get("type")
    if ("total" not in parentLowResMap):
      parentLowResMap["total"] = 0
    parentLowResMap["total"] += childLowResMap["total"]
    for childXyKey, childPixData in childLowResMap.items():
      xy = childXyKey.split("_")
      # print("xy:",xy)
      if (len(xy) == 2):  # "total" (総数入れたもの)があるので・・・
        px = px0 + int(xy[0]) // 2  # 一つ上のタイルはピクセルが半分だから
        py = py0 + int(xy[1]) // 2
        parentPixKey = str(px) + "_" + str(py)
        parentPixData = {}
        if parentPixKey not in parentLowResMap:
          parentLowResMap[parentPixKey] = [None] * (len(csvSchemaType) + 1)
          (parentLowResMap[parentPixKey])[len(csvSchemaType)] = 0  # 個数のフィールド(注意)

        parentPixData = parentLowResMap[parentPixKey]

        parentPixData[len(csvSchemaType)] += childPixData[len(csvSchemaType)]  # 個数
        for i, dType in enumerate(csvSchemaType):
          if childPixData[i] is not None:
            if dType == Csv2redisClass.T_ENUM:  # 列挙型データの統計
              if parentPixData[i] is None:
                parentPixData[i] = {}
              for key, val in childPixData[i].items():
                if (key in parentPixData[i]):
                  parentPixData[i][key] += val
                else:
                  parentPixData[i][key] = val
            elif dType == Csv2redisClass.T_NUMB:  # 数値型データの統計
              if parentPixData[i] is None:
                parentPixData[i] = []
                parentPixData[i].append(childPixData[i][0])
                parentPixData[i].append(childPixData[i][1])
                parentPixData[i].append(childPixData[i][2])
                parentPixData[i].append(childPixData[i][3])
              else:
                parentPixData[i][0] += childPixData[i][0]  # 加算値
                parentPixData[i][1] += childPixData[i][1]  # 総個数
                if (parentPixData[i][2] > childPixData[i][2]):  # min
                  parentPixData[i][2] = childPixData[i][2]
                elif (parentPixData[i][3] < childPixData[i][3]):  # max
                  parentPixData[i][3] = childPixData[i][3]

            else:  # 文字列・・
              if parentPixData[i] is None:
                parentPixData[i] = childPixData[i]
              else:
                parentPixData[i] += childPixData[i]

  def investigateKeys(self, registDataList, maxLevel):  # 高性能化の試行
    # そのPOIレコードに対応するoverflowしていないタイル番号をバースト探索する
    #    global overFlowKeys
    keys = []
    lngss = []
    latss = []
    lng0s = []
    lat0s = []

    for data in registDataList:
      keys.append("")
      lngss.append(360.0)  # これは共用で良いはず・・
      latss.append(360.0)  # 同上
      lng0s.append(-180.0)  # こっちはタイルごとに別
      lat0s.append(-180.0)  # 同上

    for i in range(maxLevel):
      pipe = self.r.pipeline()

      query2redis = {}
      queryKeyList = []  # これは要らないのかもしれない

      for j in range(len(registDataList)):
        data = registDataList[j]
        lat = data["lat"]
        lng = data["lng"]
        if (keys[j] == "") or (keys[j][-1] != ":"):  # 見つかったもの":"付与はそれ以上深堀しない

          ans0, lat0s[j], lng0s[j], latss[j], lngss[j] = self.getGeoHashCode(lat, lng, lat0s[j], lng0s[j], latss[j],
                                                                             lngss[j])
          keys[j] += ans0
          if not (keys[j] in query2redis):
            pipe.type(self.ns + keys[j])
            queryKeyList.append(keys[j])
            query2redis[keys[j]] = True

      types = pipe.execute()
      # print("types:", types, " queryKeyList:", queryKeyList)
      for j in range(len(types)):
        key = queryKeyList[j]
        query2redis[key] = types[j]

      # print(keys)

      overFlowedTilesCount = 0
      # tc = 0
      for j in range(len(registDataList)):  # zip(regidtDataList, keys)
        ans = keys[j]
        if (ans[-1] != ":"):
          # rType = types[tc]
          rType = query2redis[ans]
          # tc += 1
          if rType == b"string":  # stringのものはオーバーフローしたタイル
            overFlowedTilesCount += 1
            # overflowed tile.. should go next level
          else:  # b"list" or b"hash" or b"None"
            # null or real data tile completed..  OK
            ans += ":"
            keys[j] = ans

      if overFlowedTilesCount == 0:  # もうこれ以上掘り下げる必要はない
        break

    for j in range(len(keys)):  # :を取り除く
      keys[j] = (keys[j])[0:-1]

  #  print (keys)
    return keys

  def setDataList(self, registDataList, keys):
    # 実際にデータを登録する
    pipe = self.r.pipeline()
    for j in range(len(keys)):
      key = keys[j]
      data = registDataList[j]["data"]
      if Csv2redisClass.UseRedisHash:
        hkey = registDataList[j]["hkey"]
        pipe.hset(
            self.ns + key, hkey, data
        )  # 緯度経度も含め全データをハッシュキーにしてたのは時として丸めが起きハッシュキーとして疑問ありなので、除外したものを"hkey"に入れる(これはスタンドアロンではgetOneDataで作っている。FlaskではgetDataで作っている)
        # 緯度経度から生成されたgeoHashが1番目のredisキー、2番目のhashキーがその他のデータ(もしくはデータのID)　という形で検索できるのでそれで良いと考える
        # もしデータの緯度経度が変更されるようなデータ変更が起きる場合、データ変更前の緯度経度(もしくはgeoHash)をセットで送れば問題ないはず
      else:
        pipe.rpush(self.ns + key, data)
    ans = pipe.execute()
    # print ("setDataList:",ans,file=sys.stderr)
    return (ans)

  def checkSizes(self, keys):
    # 登録されたkeyに対するデータサイズを調査する　なんかちょっとロジックが無駄なことやってるかも？
    pipe = self.r.pipeline()
    keyList = []  # このへん 最初からdictで順番守られるんじゃないかな？
    keyDict = {}
    for key in keys:
      if key in keyDict:
        pass
      else:
        keyList.append(key)
        if Csv2redisClass.UseRedisHash:
          pipe.hlen(self.ns + key)
        else:
          pipe.llen(self.ns + key)
        keyDict[key] = True
    sizes = pipe.execute()
    keyDict = {}  #そうすればこれ以下はやらなくても勝手にできてるのでは？
    for j in range(len(sizes)):
      keyDict[keyList[j]] = sizes[j]
    # print(sizes)
    return (keyDict)

  def checkSiblingSizes(self, keys):
    # 登録されたkeyの兄弟全体のデータサイズを調査する(データ削除に伴うタイルの再統合チェック用)
    # 兄弟のうち一つでもlowResMap(redis string type)があったら、その時点でサイズオーバーとみなす
    # pipe = self.r.pipeline()
    # とりあえずパイプライン使わないで簡単実装してみる・・・
    keyDict = {}
    dataSize = 0
    for key in keys:
      if key in keyDict:
        pass
      else:
        pKey = key[0:-1]
        try:
          dataSizeA = self.r.hlen(self.ns + pKey + "A")
          dataSizeB = self.r.hlen(self.ns + pKey + "B")
          dataSizeC = self.r.hlen(self.ns + pKey + "C")
          dataSizeD = self.r.hlen(self.ns + pKey + "D")
          dataSize = dataSizeA + dataSizeB + dataSizeC + dataSizeD
        except:
          dataSize = self.listLimit + 1
        keyDict[key] = dataSize

    # print("checkSiblingSizes:",keyDict)
    return (keyDict)

  def burstQuadPart(self, dataSizes, maxLevel):
    latCol = self.schemaObj.get("latCol")
    lngCol = self.schemaObj.get("lngCol")
    for key, count in dataSizes.items():
      if (count >= self.listLimit and len(key) < maxLevel):  # fix maxLevel limitation 2019.5.22
        lat0, lng0, lats, lngs = self.geoHashToLatLng(key)
        self.quadPart(key, lat0, lng0, lats, lngs, latCol, lngCol)

  def registData(self, oneData, maxLevel):
    # global registDataList
    self.registDataList.append(oneData)
    if len(self.registDataList) < self.burstSize:
      psize = {"success": -1, "keys": []}
    else:
      psize = self.burstRegistData(self.registDataList, maxLevel)
      self.registDataList = []
    return (psize)

  def burstRegistData(self, registDataList, maxLevel):
    # 一括してデータの登録を行う（ことで高性能化している）
    # それぞれのデータがどのgeoHashに属するか調査する
    keys = self.investigateKeys(registDataList, maxLevel)
    # print ("keys:",keys)
    # print (registDataList)
    # 実際にデータを登録する
    ans = self.setDataList(registDataList, keys)
    # それぞれのタイルのデータの個数を調査する
    dataSizes = self.checkSizes(keys)  # dataSizes: dict [geoHash:size]
    # 一連の登録がすんだら、タイル再構築(quadTreeTiling)を行う
    self.burstQuadPart(dataSizes, maxLevel)

    # print ("dataSizes:",dataSizes)
    return ({"success": len(ans), "keys": keys})

  def flushRegistData(self, maxLevel):
    # バッファにたまっているデータをしっかり書き出してバッファもクリアする
    # global registDataList
    psize = self.burstRegistData(self.registDataList, maxLevel)
    self.registDataList = []
    return (psize)

  # データを１個追加する。 [[[ OBSOLUTED ]]]
  # これを呼んだだけではLowResMapは更新されない。
  # LowResMapの更新方法は２つ
  # データ１個づつ更新させたい場合 updateAncestorsLowResMap
  # 全部作り直す場合 buildAllLowResMap
  def registOneData(self, oneData, maxLevel):
    lat = oneData.lat
    lng = oneData.lng
    data = oneData.data
    # global overFlowKeys
    ans = ""  # geoHash
    lngs = 360.0
    lats = 360.0
    lng0 = -180.0
    lat0 = -180.0
    for i in range(maxLevel):
      # レベルを深めつつoverFlowしてないgeoHashタイルにストアする
      ans0, lat0, lng0, lats, lngs = self.getGeoHashCode(lat, lng, lat0, lng0, lats, lngs)
      # print(ans0,lats,lngs)
      ans += ans0
      if ans in self.overFlowKeys:
        # 下のオーバーフローキーの確認がかなり重たい rev2と比べてrev3が８倍ぐらい重いのを改善
        o = True
      else:  ############################ この辺　今作業中です！！！
        rType = self.r.type(self.ns + ans)
        if rType == b"string":
          # overflowed lowResMap
          # overflowしたキーは、バイナリ（ストリング）データとして固有オブジェクト構造を保存している
          self.overFlowKeys[ans] = True
          print("add over flow keys: " + ans, file=sys.stderr)
        elif rType == b"list":
          ## realData
          tileDataSize = self.r.llen(self.ns + ans)
          if tileDataSize < self.listLimit:
            # overflowしてないキーはlist構造で個々のPOIをCSV文字列で保存している
            #          self.r.rpush(ans, data)
            break
          else:
            # ちょうどオーバーフローしたところでは、下の階層を作ってまずは分割（ループは終わらないのでその次のループで実際に下の階層にデータをストアする
            self.quadPart(ans, lat0, lng0, lats, lngs, self.schemaObj.get("latCol"), self.schemaObj.get("lngCol"))
        else:  # Noneすなわちキーが存在しない時は追加
          #        self.r.rpush(ans, data)
          break

  #  print(".", end='')
  #  print ("Regist: Key:", ans ," Val:",data)

  def deleteData(self, oneData, maxLevel):
    # global deleteDataList
    self.deleteDataList.append(oneData)
    if len(self.deleteDataList) < self.burstSize:
      psize = {"success": -1, "keys": []}
    else:
      psize = self.burstDeleteData(self.deleteDataList, maxLevel)
      self.deleteDataList = []
    return (psize)

  def flushDeleteData(self, maxLevel):
    # バッファにたまっているデータ分も消去してバッファもクリアする
    # global deleteDataList
    psize = self.burstDeleteData(self.deleteDataList, maxLevel)
    self.deleteDataList = []
    return (psize)

  def burstDeleteData(self, deleteDatas, maxLevel):
    # 一括してデータの削除を行う
    keys = self.investigateKeys(deleteDatas, maxLevel)
    ans = self.delDataList(deleteDatas, keys)
    dataSizes = self.checkSiblingSizes(keys)  # dataSizes: dict [geoHash:size]
    print("burstDeleteData dataSizes:", dataSizes, file=sys.stderr)
    self.burstCombine(dataSizes)
    return ({"success": len(ans), "keys": keys})

  def burstCombine(self, dataSizes):
    # 兄弟の合計がリミットを下回っていたら上のタイルに統合してしまう
    # ただしすべての兄弟が実データ（lowResMapでない）の場合に限る
    # pipe版
    # ISSUE: カラのタイルが残存してしまう場合がある　が、updateLowResMapで消せるはず
    combinedKeys = {}
    for key, count in dataSizes.items():
      if (len(key) > 1 and count < self.listLimit):
        pKey = key[0:-1]
        if pKey in combinedKeys:
          pass
        else:
          pipe = self.r.pipeline()
          combinedKeys[pKey] = True
          print("burstCombine:", key, "->", pKey, file=sys.stderr)
          pipe.hgetall(self.ns + pKey + "A")
          pipe.hgetall(self.ns + pKey + "B")
          pipe.hgetall(self.ns + pKey + "C")
          pipe.hgetall(self.ns + pKey + "D")
          pipe.delete(self.ns + pKey + "A")
          pipe.delete(self.ns + pKey + "B")
          pipe.delete(self.ns + pKey + "C")
          pipe.delete(self.ns + pKey + "D")
          pipe.delete(self.ns + pKey)
          dats = pipe.execute()
          dat = dats[0]
          dat.update(dats[1])
          dat.update(dats[2])
          dat.update(dats[3])
          if len(dat) > 0:
            self.r.hmset(self.ns + pKey, dat)

  def delDataList(self, deleteDatas, geoHashKeys):
    pipe = self.r.pipeline()
    for j in range(len(geoHashKeys)):
      geoHashKey = geoHashKeys[j]
      hkey = deleteDatas[j]["hkey"]
      if Csv2redisClass.UseRedisHash:
        print("hdel", geoHashKey, hkey)
        pipe.hdel(self.ns + geoHashKey, hkey)
      else:
        pass
    ans = pipe.execute()
    print("delDataList pipe::: ans:", ans, "  deleteDatas:", deleteDatas, " geoHashKeys:", geoHashKeys, file=sys.stderr)
    return (ans)

  def saveAllSvgMap(self, lowResImage=False):
    # global r
    bkeys = self.r.keys(self.ns + "[A-D]*")
    pipe = self.r.pipeline()
    for key in bkeys:
      pipe.type(key)
    types = pipe.execute()

    for j in range(len(bkeys)):
      key = (bkeys[j].decode())[len(self.ns):]
      dtype = types[j]
      # key = key.decode()
      self.saveSvgMapTileN(key, dtype, lowResImage)
      #    saveSvgMapTile(key, dtype) # 性能が悪いのでobsolute・・・
      if (j % 20 == 0):
        print(100 * (j / len(bkeys)), "%", file=sys.stderr)

  def getSchemaTypeStrArray(self, csvSchemaType):
    ans = []
    for sn in csvSchemaType:
      ans.append(Csv2redisClass.scehmaTypeStr[sn])
    return (ans)

  def getMetaExclLatLng(self, strList, latCol, lngCol):  # 配列から緯度経度カラムを除いたカンマ区切り文字列を作ります
    ans = []
    for i, val in enumerate(strList):
      if (i == latCol or i == lngCol):
        pass
      else:
        ans.append(val)

    return (ans)

  def saveSvgMapTileN(
      self,
      geoHash=None,  # タイルハッシュコード
      dtype=None,  # あらかじめわかっている場合のデータタイプ(低解像タイルか実データタイル化がわかる)
      lowResImage=False,  # 低解像タイルをビットイメージにする場合
      onMemoryOutput=False,  # ライブラリとして使用し、データをオンメモリで渡す場合
      returnBitImage=False):  # オンメモリ渡し(上がTrue限定)のとき、低解像ビットイメージデータを要求する場合
    # saveSvgMapTileを置き換え、SVGMapコンテンツをSAX的に直生成することで高速化を図る　確かに全然早くなった。pythonってやっぱりゆる系？・・・ 2019/1/16
    # outStrL = []  # 出力するファイルの文字列のリスト　最後にjoinの上writeする

    # global r
    # global poi_color, poi_index, poi_size

    print("saveSvgMapTileN: schemaObj:", self.schemaObj)

    latCol = self.schemaObj.get("latCol")
    lngCol = self.schemaObj.get("lngCol")
    csvSchema = self.schemaObj.get("schema")
    csvSchemaType = self.schemaObj.get("type")
    titleCol = self.schemaObj.get("titleCol")
    #    print(dtype)
    #print(csvSchema)
    #print([str] * len(csvSchema))
    svgc = SvgmapContainer(
        self.getMetaExclLatLng(csvSchema, latCol, lngCol),
        self.getMetaExclLatLng(self.getSchemaTypeStrArray(csvSchemaType), latCol, lngCol))
    svgc.regist_size(self.poi_size)
    svgc.regist_defs(self.poi_color)
    svgc.color_column_index = self.poi_index

    if geoHash is None or geoHash == "":  # レベル0のタイルをgeoHash=Noneで作るようにした2019/2/26
      dtype = b"string"
      lat0 = -180
      lng0 = -180
      lats = 360
      lngs = 360
      geoHash = ""
    else:
      lat0, lng0, lats, lngs = self.geoHashToLatLng(geoHash)
      thisZoom = len(geoHash)

    if dtype is None:
      dtype = self.r.type(self.ns + geoHash)

    if dtype == b"string":  # そのタイルはオーバーフローしている実データがないlowRes pickleタイル
      if lats < 360:  # レベル0のタイル(レイヤールートコンテナ)じゃない場合はそのレベルの低解像度タイルを入れる
        pixW = 100 * lngs / self.lowresMapSize
        pixH = 100 * lats / self.lowresMapSize
        g = Tag('g')
        g.fill, g.visibleMaxZoom = (
            'blue',
            Csv2redisClass.topVisibleMinZoom * pow(2, thisZoom - 1),
        )

        # bitImage出力 http://d.hatena.ne.jp/white_wheels/20100322/p1
        if lowResImage:
          if onMemoryOutput and not returnBitImage:
            pass  # ただし、オンメモリ生成でビットイメージ要求がない場合はビットイメージ生成必要ない
          else:
            bitImage = np.zeros([self.lowresMapSize, self.lowresMapSize, 4], dtype=np.uint8)
            bitImage[:, :] = [0, 0, 0, 0]  # black totally transparent

        if lowResImage and (onMemoryOutput and not returnBitImage):
          pass  # オンメモリ生成でビットイメージ要求がない場合、しかもlowResImageの場合は低分解能コンテンツ生成の必要はない(コンテナ作るだけ)
        else:
          thisTile = pickle.loads(self.r.get(self.ns + geoHash))
          # print(geoHash, "lowRes Data:len", len(thisTile), thisTile)
          # print(geoHash, "lowRes Data:len", len(thisTile))
          for xyKey, data in thisTile.items():
            xy = xyKey.split("_")
            if (len(xy) == 2):
              x = int(xy[0])
              y = int(xy[1])
              # print(x,y,xyKey,data)
              if (lowResImage):
                yi = self.lowresMapSize - y - 1
                bitImage[yi, x, 2] = 255  # blue=255
                bitImage[yi, x, 3] = 255  # alpha=255

              else:
                lng = lng0 + lngs * (x / self.lowresMapSize)
                lat = lat0 + lats * (y / self.lowresMapSize)
                title = xyKey
                item = Tag('rect')
                item.x, item.y, item.width, item.height = (100 * lng, -100 * lat - pixH, pixW, pixH)
                item.content = 'totalPois:%s' % len(csvSchemaType)
                g.append_child(item)
        print(g)

        if (lowResImage):  # 作ったpngを参照するimageタグを作る
          if onMemoryOutput and not returnBitImage:
            pass  # ただし、オンメモリ生成でビットイメージ要求がない場合はビットイメージ生成必要ない
          else:
            img = Image.fromarray(bitImage)
            if (returnBitImage):  # オンメモリでのビットイメージ出力要求
              img_io = BytesIO()
              img.save(img_io, "PNG")
              #img = img.convert('RGB') # jpegの場合はARGB受け付けてくれない
              #img.save(img_io, 'JPEG', quality=70)
              img_io.seek(0)
              return (img_io)  # ちょっと強引だがここで出力して終了
            if not onMemoryOutput:
              img.save(self.targetDir + Csv2redisClass.svgFileNameHd + geoHash + ".png")
          image = Tag("image")
          image.style = 'image-rendering:pixelated'
          image.__setattr__("xlink:href", Csv2redisClass.svgFileNameHd + geoHash + ".png")
          image.x, image.y, image.width, image.height = (100 * (lng0), -100 * (lat0 + lats), 100 * lngs, 100 * lats)
          g.append_child(image)
        svgc.add_tag(g)

      pipe = self.r.pipeline()  # パイプ使って少し高速化できたか？
      pipe.exists(self.ns + geoHash + "A")
      pipe.exists(self.ns + geoHash + "B")
      pipe.exists(self.ns + geoHash + "C")
      pipe.exists(self.ns + geoHash + "D")
      ceFlg = pipe.execute()

      g = Tag('g')

      if lats < 360:
        g.fill = 'blue'
        g.visibleMinZoom = Csv2redisClass.topVisibleMinZoom * pow(2, thisZoom - 1)
      else:  # レベル0のレイヤルートコンテナの場合 (このルーチン　まずくない？)
        #outStrL.append(
        # このdefsはオーサリングシステムのアイコンがレイヤールートのdefsを参照していることによるので、何もアイコンはないけどしっかりdefsしておく必要がある！
        # y.sakiuraさんのシステムでは、無条件で共通のdefsを全部のタイルに置くようになっているので大丈夫になってるからパスする
        #    "<defs>\n <g id='p0'>\n  <image height='27' preserveAspectRatio='none' width='19' x='-8' xlink:href='mappin.png' y='-25'/>\n </g>\n</defs>\n"
        #)
        #outStrL.append("<g>\n")
        pass

      for i, exs in enumerate(ceFlg):  # link to child tiles
        cN = chr(65 + i)
        childGeoHash = geoHash + cN
        #      print("EXISTS?", cN,exs)
        if (exs):
          ani = Tag("animation")
          ani.__setattr__("xlink:href", Csv2redisClass.svgFileNameHd + childGeoHash + ".svg")
          lat_shift = 0
          lng_shift = 0
          if cN == "B":
            lng_shift = lngs / 2
          elif cN == "C":
            lat_shift = lats / 2
          elif cN == "D":
            lng_shift = lngs / 2
            lat_shift = lats / 2
          # 緯度・lats/2の足し方ちょっと怪しい・・・
          ani.x, ani.y, ani.width, ani.height = (100 * (lng0 + lng_shift), -100 * (lat0 + lat_shift + lats / 2),
                                                 100 * lngs / 2, 100 * lats / 2)
          g.append_child(ani)
      svgc.add_tag(g)

    else:  # 実データ
      # raw data
      if dtype == b"list":
        src = self.r.lrange(self.ns + geoHash, 0, -1)  # 全POI取得
      else:
        src = self.r.hgetall(self.ns + geoHash)
        src = list(src.values())

      print(geoHash, "real Data:len", len(src), file=sys.stderr)

      outPoiL = []

      for poidata in src:
        poi = poidata.decode().split(',', -1)
        lat = float(poi[latCol])
        lng = float(poi[lngCol])
        if (titleCol >= 0):
          title = poi[titleCol]
        else:
          title = poi[0]
        # print("titleCol :::::::", titleCol, "     title:", title, "   poi:", poi)

        if (self.customDataGenerator is None):
          svgc.add_content(title, poi[latCol], poi[lngCol], self.getMetaExclLatLng(poi, latCol, lngCol))
        else:
          poiObj = {"lat": lat, "lng": lng, "title": title, "metadata": self.getMetaExclLatLng(poi, latCol, lngCol)}
          outPoiL.append(poiObj)

    #print(svgc.output_str_to_container())

    if (onMemoryOutput):  # 文字列として返却するだけのオプション
      #return "".join(outStrL)
      if (self.customDataGenerator is None):
        return svgc.output_str_to_container()
      else:
        return self.customDataGenerator.outputSvgContent(outPoiL, svgc)
    else:
      with open(self.targetDir + Csv2redisClass.svgFileNameHd + geoHash + ".svg", mode='w', encoding='utf-8') as f:
        # f.write("".join(outStrL))  # writeは遅いらしいので一発で書き出すようにするよ
        if (self.customDataGenerator is None):
          f.write(svgc.output_str_to_container())
        else:
          f.write(self.customDataGenerator.outputSvgContent(outPoiL, svgc))
        # f.flush() # ひとまずファイルの書き出しはシステムお任せにしましょう・・

  def xmlEscape(self, str):
    ans = str.replace("'", "&apos;")
    ans = ans.replace('"', "&quot;")
    ans = ans.replace("&", "&amp;")
    return (ans)

  def init(self, redisNs="svgMap:"):
    # global r, schemaObj, ns

    self.ns = redisNs

    #if (isinstance(self.r, redis.Redis)):
    #  print("Skip redis gen")
    #  pass
    #else:
    #  self.r = redis.Redis(host='localhost', port=6379, db=0)

    if (len(self.schemaObj.get("schema")) == 0 or self.schemaObj.get("namespace") != self.ns):
      if self.r.exists(self.ns + "schema"):
        self.schemaObj = pickle.loads(self.r.get(self.ns + "schema"))
        print("[[[INIT]]]   load schemaObj:", self.schemaObj, "  NS:", redisNs)
        #schemaObj={
        #  "schema" : schemaObj.get("schema"),
        #  "type" : schemaObj.get("type"),
        #  "latCol" : schemaObj.get("latCol"),
        #  "lngCol" : schemaObj.get("lngCol"),
        #  "titleCol": schemaObj.get("titleCol")
        #}
    else:
      print("[[[INIT]]]    SKIP load schema : NS: ", redisNs, file=sys.stderr)
      pass

  def getSchema(self, header):
    # CSVファイルのヘッダ行からスキーマを取得する
    # http://programming-study.com/technology/python-for-index/
    # 最初の行はカラム名とその型（スキーマを獲得する）（ToDo: サブルーチン化）
    csvSchema = []
    csvSchemaType = []
    latCol = -1
    lngCol = -1
    titleCol = -1
    for i, hdname in enumerate(header):
      if (hdname.find("東経") >= 0 or hdname.lower().find("longitude") >= 0 or hdname.find("経度") >= 0) and lngCol == -1:
        lngCol = i
        csvSchema.append("longitude")
        csvSchemaType.append(Csv2redisClass.T_NUMB)
      elif (hdname.find("北緯") >= 0 or hdname.lower().find("latitude") >= 0 or hdname.find("緯度") >= 0) and latCol == -1:
        latCol = i
        csvSchema.append("latitude")
        csvSchemaType.append(Csv2redisClass.T_NUMB)
      elif (hdname.lower().find("title") >= 0 or hdname.lower().find("name") >= 0 or
            hdname.find("名称") >= 0) and titleCol == -1:
        titleCol = i
        # titleColはメタデータとしてもstring型として作っておくことにします
        csvSchema.append(hdname)
        csvSchemaType.append(Csv2redisClass.T_STR)
      else:
        csvSchema.append(hdname)
        if hdname.lower().endswith(":e"):
          csvSchemaType.append(Csv2redisClass.T_ENUM)
        elif hdname.lower().endswith(":s"):
          csvSchemaType.append(Csv2redisClass.T_STR)
        elif hdname.lower().endswith(":n"):
          csvSchemaType.append(Csv2redisClass.T_NUMB)
        else:
          csvSchemaType.append(Csv2redisClass.T_STR)

    csvSchemaObj = {
        "schema": csvSchema,
        "type": csvSchemaType,
        "latCol": latCol,
        "lngCol": lngCol,
        "titleCol": titleCol,
        "idCol": -1,
        "namespace": "",
        "name": "default"
    }

    return csvSchemaObj

  def getCsvReader(self, inputcsv):
    csv_file = open(inputcsv, "r", encoding="utf-8", errors="", newline="")
    # リスト形式
    file = csv.reader(
        csv_file, delimiter=",", doublequote=True, lineterminator="\r\n", quotechar='"', skipinitialspace=True)
    return file

  def readAndRegistData(self, file, latCol, lngCol, maxLevel):
    # 二行目以降がデータになる～実データを読み込む（ToDo: サブルーチン化）
    lines = 0
    for row in file:
      # rowはList
      # row[0]で必要な項目を取得することができる
      #     print(row)
      oneData = self.getOneData(row, latCol, lngCol)
      self.registData(oneData, maxLevel)
      lines = lines + 1
      if (lines % 1000 == 0):
        print(lines, file=sys.stderr)

    self.flushRegistData(maxLevel)  # バッファを全部書き出す

  def readAndDeleteData(self, file, latCol, lngCol, maxLevel):
    # 二行目以降がデータになる～実データを読み込む（ToDo: サブルーチン化）
    lines = 0
    for row in file:
      # rowはList
      # row[0]で必要な項目を取得することができる
      #     print(row)
      oneData = self.getOneData(row, latCol, lngCol)
      self.deleteData(oneData, maxLevel)
      lines = lines + 1
      if (lines % 1000 == 0):
        print(lines, file=sys.stderr)

    self.flushDeleteData(maxLevel)  # バッファを全部消去処理して完了させる

  def validateData(self, dataStr, index):  # スキーマと照合して整合性チェックする　実質は数値のみ
    csvSchemaType = self.schemaObj.get("type")
    ans = False
    if csvSchemaType[index] == Csv2redisClass.T_ENUM:
      ans = True
    elif csvSchemaType[index] == Csv2redisClass.T_NUMB:
      if self.is_float(dataStr):
        ans = True
      # print ( "NUM:",dataStr,ans)
    elif csvSchemaType[index] == Csv2redisClass.T_STR:
      ans = True
    # print ( "validateData:",dataStr,index,csvSchemaType[index] ,ans)
    return ans

  def is_float(self, s):
    try:
      float(s)
    except:
      return False
    return True

  def getOneData(self, row, latCol, lngCol, idCol=-1):
    csvSchema = self.schemaObj.get("schema")
    # csv１行分の配列から、登録用のデータを構築する。ここで、データの整合性もチェックする
    # Flaskによるウェブサービスではこの関数は使ってない(2019.5.14)
    lat = float(row[latCol])
    lng = float(row[lngCol])
    meta = ""
    dHkey = ""
    for i, data in enumerate(row):
      if (self.validateData(data, i)):
        meta += str(data)
        if (i == latCol):
          dHkey += str(math.floor(lat * 100000))
        elif (i == lngCol):
          dHkey += str(math.floor(lng * 100000))
        else:
          dHkey += str(data)
      else:
        meta += "-"
        dHkey += "-"
      if i < len(csvSchema) - 1:
        meta += ","
        dHkey += ","

    # print("ParsedCsvData:",lat,lng,meta)
    if (idCol != -1):
      hkey = row[idCol]
    elif (idCol == latCol or idCol == lngCol):  # idカラムに緯度化経度が明示された場合は、緯度:経度 それぞれ5桁目まででハッシュ(ID)とする
      hkey = str(math.floor(lat * 100000)) + ":" + str(math.floor(lng * 100000))
    else:
      hkey = dHkey

    # print ("getOneData hkey:",hkey, file=sys.stderr)

    oneData = {"lat": lat, "lng": lng, "data": meta, "hkey": hkey}  # hkeyで実データのハッシュを直に指定 2019/3/13
    # print("oneData:", oneData, " row:", row)
    return (oneData)

  def deleteAllData(self, flushSchema=False):
    #    global r
    i = 0
    pipe = self.r.pipeline()
    for key in self.r.scan_iter(self.ns + "[A-D]*"):
      pipe.delete(key)
      i += 1
    if (flushSchema):
      print("remove dataset")
      pipe.delete(self.ns + "schema")
      pipe.hdel("dataSet", self.ns)
    pipe.execute()
    print("deleted.", i, "  ns:", self.ns)
    return i

  def registSchema(self, schemaData):
    #    global r, schemaObj, ns
    # r = redis.Redis(host='localhost', port=6379, db=0)
    self.ns = schemaData.get("namespace")
    if (self.r.exists(self.ns + "schema")):
      return (False)
    self.r.set(self.ns + "schema", pickle.dumps(schemaData))
    self.r.hset("dataSet", self.ns, schemaData.get("name"))
    self.schemaObj = schemaData
    return (True)

  def listSubLayers(self):
    #   global r
    # r = redis.Redis(host='localhost', port=6379, db=0)
    # print(self.r.hgetall("dataSet"))
    return (self.r.hgetall("dataSet"))

  def regist_poi_size(self, _size: list):
    # global poi_size
    self.poi_size = _size

  def regist_poi_color(self, _color: list):
    # global poi_color
    self.poi_color = _color

  def regist_poi_index(self, _index: int):
    # global poi_index
    self.poi_index = _index

  def poi_init(self, _size: list, _color: list, _index: int):
    '''
    POIの実データ生成時のイメージの登録
    '''
    self.regist_poi_size(_size)
    self.regist_poi_color(_color)
    self.regist_poi_index(_index)


# END OF CLASS


def main():
  #  global r, targetDir, lowResIsBitImage, schemaObj

  parser = argparse.ArgumentParser()
  parser.add_argument("--input")
  parser.add_argument("--append")
  parser.add_argument("--dir")
  parser.add_argument("--lowres")
  parser.add_argument("--delete")
  parser.add_argument("--onlysaveallmap", action='store_true')
  parser.add_argument("--onlybuildlowres", action='store_true')
  parser.add_argument("--debug", action='store_true')
  parser.add_argument("--saveallmap", action='store_true')
  parser.add_argument("--ns")
  # 色分けに関するオプション追加 2019.06.05 Yutaka Sakiura
  parser.add_argument("--imagecolumn")
  parser.add_argument("--opacity")
  parser.add_argument("--image")
  parser.add_argument("--size")

  dbns = "s2_"

  inputcsv = "./worldcitiespop_jp.csv"
  args = parser.parse_args()
  newcsv = args.input
  appendcsv = args.append
  deletecsv = args.delete
  if (args.ns):
    dbns = args.ns

  csv2redis = Csv2redisClass()

  csv2redis.init(dbns)

  appendMode = False
  deleteMode = False

  targetDir = "mapContents"
  if (args.dir):
    targetDir = args.dir

  if (targetDir != "" and targetDir[-1:] != "/"):
    targetDir = targetDir + "/"

  csv2redis.targetDir = targetDir

  lowResIsBitImage = True
  if (args.lowres == "vector"):
    lowResIsBitImage = False

  if (newcsv):
    inputcsv = newcsv
  elif (appendcsv):
    print("APPEND MODE using : ", appendcsv, file=sys.stderr)
    inputcsv = appendcsv
    appendMode = True
  elif (deletecsv):
    print("DELETE MODE using : ", deletecsv, file=sys.stderr)
    inputcsv = deletecsv
    deleteMode = True

  print("rebuildFromFile:", inputcsv, "  appendFromFile:", appendcsv, " appendMode:", appendMode, file=sys.stderr)

  if (args.debug):
    csv2redis.saveSvgMapTileN("BDDD", None, True)
    return

  if (args.onlysaveallmap):
    csv2redis.saveAllSvgMap(lowResIsBitImage)
    return

  if (args.onlybuildlowres):
    csv2redis.buildAllLowResMap()
    return

  if (not appendMode and not deleteMode):
    print("FlushDB  nameSpace:" + csv2redis.ns, file=sys.stderr)
    # r.flushdb()  # DBを消去する これだと全部消してしまう(ns関係なく)ので・・
    csv2redis.deleteAllData()

  # csvファイルリーダーを作る
  file = csv2redis.getCsvReader(inputcsv)

  header = next(file)
  # ヘッダからスキーマを取得
  csvSchemaObj = csv2redis.getSchema(header)
  # print ( "build schema:", csvSchemaObj["csvSchema"]," type:",csvSchemaObj["csvSchemaType"]," lat:",latCol," lng:", lngCol," title:",titleCol)
  csvSchemaObj["namespace"] = csv2redis.ns

  if (appendMode or deleteMode):
    pass
  else:
    # スキーマレコードは必ず新しく作った時だけに生成するようにしておく　ねんのため
    # schemaObj = csvSchemaObj
    print("csvSchemaObj:", csvSchemaObj, file=sys.stderr)
    # r.set(ns + "schema", pickle.dumps(csvSchemaObj))
    csv2redis.registSchema(csvSchemaObj)

  print(
      "latCol,lngCol,csvSchema, csvSchemaType:",
      csv2redis.schemaObj.get("latCol"),
      csv2redis.schemaObj.get("lngCol"),
      csv2redis.schemaObj.get("schema"),
      csv2redis.schemaObj.get("type"),
      file=sys.stderr)

  # CSVファイルを読み込みデータをredisに登録する
  latCol = csv2redis.schemaObj.get("latCol")
  lngCol = csv2redis.schemaObj.get("lngCol")
  if (deleteMode):
    csv2redis.readAndDeleteData(file, latCol, lngCol, csv2redis.maxLevel)
  else:
    csv2redis.readAndRegistData(file, latCol, lngCol, csv2redis.maxLevel)

  # 低解像度タイルデータベースを全部一から生成する
  csv2redis.buildAllLowResMap()

  # printAllLowResMap()

  # SVGMapコンテンツ(オプションによって低解像ビットイメージ)を全部一から生成する
  if (args.saveallmap):
    csv2redis.saveAllSvgMap(lowResIsBitImage)

  print("schema:", pickle.loads(csv2redis.r.get(csv2redis.ns + "schema")), file=sys.stderr)


if __name__ == "__main__":
  main()
