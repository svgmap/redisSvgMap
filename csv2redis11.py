import sys
from io import BytesIO
import csv
import codecs
from xml.dom import minidom
import argparse
import redis
import pickle
# import csv2svgmap
from PIL import Image
import numpy as np
import hashlib

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
# ISSUES 動的出力で小縮尺ビットイメージモードの場合は、もっと効率化できる（ビットイメージ生成はSVGファイル要求時は不要なので）

#global 変数たち
r = {}
listLimit = 500
lowresMapSize = 128  # 集約データの分解能の定義(この数値のスクエア)　この実装では2の倍数である必要がある(上の階層の倍という意味で)
csvSchema = []
csvSchemaType = []
latCol = -1
lngCol = -1

overFlowKeys = {}  # quadPartを実行し、子供の階層を構築したメッシュ （オンメモリであり永続化されているわけではない）
updatedLrMapKeys = {}  # 上のdictで足りると思うが、テストのために実装してみる　pointを追加したとき、そのpointを直上のlrMapのdictを入れる　今後恐らく一点単位の登録が発生すると意味が出てくる？

maxLevel = 16

# static for csvSchemaType
T_ENUM = 0
T_NUMB = 1
T_STR = 2

svgFileNameHd = "svgMapTile"
topVisibleMinZoom = 4.5

targetDir = "mapContents"  # コンテンツ出力ディレクトリ
lowResIsBitImage = True

UseRedisHash = True  # これはもはやTrue固定です　あとでFalseケースの実装を外します 2019/3/13


def registLrMap(lrMap, xyKey, splitedData):
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
      if csvSchemaType[i] == T_ENUM:  # 列挙型データの統計
        if pixData[i] == None:
          pixData[i] = {}
        if (val in pixData[i]):
          pixData[i][val] += 1
        else:
          pixData[i][val] = 1
      elif csvSchemaType[i] == T_NUMB:  # 数値型データの統計　標準偏差とかもあると良いと思うが・・
        if (pixData[i] == None):
          pixData[i] = []
          pixData[i].append(float(val))
          pixData[i].append(1)
        else:
          pixData[i][0] += float(val)
          pixData[i][1] += 1
      else:  # 文字列型　あまり意味ない　とりあえず総文字数カウントでもしとくか・・
        if (pixData[i] == None):
          pixData[i] = len(val)
        else:
          pixData[i] += len(val)


def addLowResMap(targetGeoHash, lat, lng, poidata, lrMap, lat0, lng0, lats, lngs):
  # poidata：split済みのメタデータ文字列　ただし０，１番目に緯度経度入り
  # lat0, lng0, lats, lngs = geoHashToLatLng(targetGeoHash)
  # print("\nbuildLowResMap: targetLatLng:", lat0, lng0, lats, lngs)
  if lat >= 90.0:
    lat = 89.9999999
  if lng >= 180.0:
    lng = 179.9999999

  lati = int(lowresMapSize * (lat - lat0) / lats)
  lngi = int(lowresMapSize * (lng - lng0) / lngs)
  hKey = str(lngi) + "_" + str(lati)  # x_y : 経度方向のピクセル_緯度方向のピクセル
  if (lati >= lowresMapSize or lngi >= lowresMapSize):
    print(lati, lngi, lat, lng, lats, lngs, lat0, lng0, file=sys.stderr)
    raise NameError('outOfRangeErr')
  # print("addLowResMap", hKey)
  registLrMap(lrMap, hKey, poidata)


def geoHashToLatLng(hash):
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


def getGeoHashCode(lat, lng, lat0, lng0, lats, lngs):
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


def quadPart(ans, lat0, lng0, lats, lngs):
  # 登録個数の上限に達したタイルのデータを４分割
  # 分割後にLowResMapを構築するのは、一連の全データの追加後に行うこととしてみる
  # そのかわり、データ追加に伴って影響を受けたLowResmapのhashをオンメモリに貯めることにしてみる（これはこの関数内ではなく、redisに個々のデータを追加している段階で行うこと）
  global r, overFlowKeys
  if UseRedisHash:
    src = list((r.hgetall(ans)).values())
    hKeys = list((r.hgetall(ans)).keys())
  else:
    src = r.lrange(ans, 0, -1)  # 分割元のデータ

  #以下pipelineを使うと早くなるかな
  pipe = r.pipeline()
  # lowResMap = {}  # ここに、最大lowresMapSize^2のhashKey数で低解像度用のメッシュ型カバレッジデータ構造が構築される
  count = 0
  for poidata in src:
    #    print ("quadPart",data.decode())
    poi = poidata.decode().split(',', -1)
    lat = float(poi[0])
    lng = float(poi[1])
    del poi[0:2]
    # addLowResMap(ans, lat, lng, poi, lowResMap, lat0, lng0, lats, lngs) # このタイミングで行う必要はないと思う　一連のデータ登録が完了したタイミングで行うのが良いと思う。
    ans0, latN0, lngN0, latNs, lngNs = getGeoHashCode(lat, lng, lat0, lng0, lats, lngs)
    key = ans + ans0
    if UseRedisHash:
      hkey = hKeys[count]
      pipe.hset(key, hkey, poidata)
      count = count + 1
    else:
      pipe.rpush(key, poidata)

  pipe.set(ans, "OVERFLOW")
  pipe.execute()
  # storeLowResDataToRedis(r, ans, lowResMap) ということで、
  overFlowKeys[ans] = True
  print("\nEnd QuadPart:", ans, file=sys.stderr)


def storeLowResDataToRedis(r, key, lowResMap):
  # use pickle http://www.denzow.me/entry/2017/10/08/212059
  # print("storeLowResDataToRedis", key, "  map:", lowResMap)
  r.set(key, pickle.dumps(lowResMap))
  #ToDo: 上の階層に遡ってLowResを更新する必要がある
  #issue: この処理はredisアクセスおよびpickleパースが必要なのでpython上でキャッシュできると良いかも？
  # parentKey = key
  # childLowResMap = lowResMap


def updateAncestorsLowResMap(key):
  # 指定したLowResMap以上のものを更新する(そのポイントを子孫に持つものだけを生成しなおしなので少し早いと思う)
  # データ1個を追加した後に、そのデータの一つ上の階層のLowResMapを指定すれば、全LowResMapが更新したデータを反映したものになる
  print("updateAncestorsLowResMap", key, file=sys.stderr)
  for i in range(len(key)):
    if (i == 0):
      sKey = key
    else:
      sKey = key[: - i]  # もうちょっといいループの作り方あるでしょうね・・・

    # print("sKey:", sKey, r.type(sKey.encode()), file=sys.stderr)
    if (r.type(sKey.encode()) == b"string"):
      # print("update LowResMap sKey", sKey, file=sys.stderr)
      thisTile = {}
      childTiles = []
      childTiles.append(getChildData(sKey + "A"))
      childTiles.append(getChildData(sKey + "B"))
      childTiles.append(getChildData(sKey + "C"))
      childTiles.append(getChildData(sKey + "D"))
      updateLowResMap(sKey, thisTile, childTiles)


def buildAllLowResMap():
  # 全LowResMapを一から生成しなおす（元のLowResMapデータがあっても利用せず上書き)
  global r
  bkeys = r.keys("[A-D]*")
  bkeys.sort(key=len)  # 下のレベルのLowResMapから更新して上のレベルに波及させる必要があるのでgeohash長い順ソートする
  # print ( "buildAllLowResMap: Keys:",bkeys )

  for key in reversed(bkeys):  # 下のレベルから
    key = key.decode()
    if r.type(key) == b"string":  # そのタイルはオーバーフローしている実データがないタイル　実際にはpickleでバイナリ化したデータが入っているかb"OVERFLOW"がただ入ってる
      print(key, "STR", file=sys.stderr)
      thisTile = {}
      # thisTile = r.get(key)
      # if thisTile == b"OVERFLOW": # 更新ではなくて新規なのでこれは不要
      #  thisTile = {}
      #else:
      #  thisTile = pickle.loads(thisTile)

      childTiles = []
      childTiles.append(getChildData(key + "A"))
      childTiles.append(getChildData(key + "B"))
      childTiles.append(getChildData(key + "C"))
      childTiles.append(getChildData(key + "D"))

      updateLowResMap(key, thisTile, childTiles)

    else:  # そのタイルは実データが入っている(b"list")のデータ
      print("This is real data:", key, r.type(key), file=sys.stderr)


def printAllLowResMap():
  global r
  bkeys = r.keys("[A-D]*")
  bkeys.sort(key=len)
  print("printAllLowResMap: Keys:", bkeys, file=sys.stderr)

  for key in reversed(bkeys):
    key = key.decode()
    if r.type(key) == b"string":  # そのタイルはオーバーフローしている実データがないタイル　実際にはpickleでバイナリ化したデータが入っているかb"OVERFLOW"がただ入ってる
      thisTile = pickle.loads(r.get(key))
      print("Key:", key, thisTile, file=sys.stderr)


def getChildData(key):
  dType = r.type(key)  # 高性能化のためexistsを排除
  if dType == b"string":  # overflowed lowResImage
    # print("child data is overflowtile ::: ", key)
    return (pickle.loads(r.get(key)))
  elif dType == b"list":  # real data
    return (r.lrange(key, 0, -1))
  elif dType == b"hash":  # real data by Hash 2019/2/19
    return (list((r.hgetall(key)).values()))
  else:
    return (None)


#    print(key, r.type(key))
#    if ( r.type(key.decode()))
#  for key in bkeys:
#    keys.append(key.decode())
#
#  keys.sort(key=len)
#  print (keys)


def updateLowResMap(geoHash, lowResMap, childTiles):
  # その階層のLowResMapを更新する
  # この関数は再帰的に上に伸ばしていく処理の中で使われるはず 12/12
  # geoHash: そのlowResMapのgeoHashタイル番号
  # lowResMap: その階層のLowResMap
  # childTilsDatas: その階層の１段下の子供( 0..3 : A,B,C,D) lowResMapの場合もあるし、実データの場合もある
  # print("updateLowResMap : len:childTiles:", len(childTiles), file=sys.stderr)
  for i, childTile in enumerate(childTiles):
    if (childTile):
      if "total" in childTile:  # childもloweResMap
        px0 = 0
        py0 = 0
        if i == 1:
          px0 = lowresMapSize // 2
        elif i == 2:
          py0 = lowresMapSize // 2
        elif i == 3:
          px0 = lowresMapSize // 2
          py0 = lowresMapSize // 2
        updateLowResMapSub(lowResMap, childTile, px0, py0)
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
        lat0, lng0, lats, lngs = geoHashToLatLng(geoHash)  # debug geoHash: childじゃないのが正しいぞ 2019.1.8
        updateLowResMapData(lowResMap, childTile, lat0, lng0, lats, lngs)

  print("updateLowResMap: set lowResData: ", geoHash, file=sys.stderr)
  if ("total" in lowResMap):
    r.set(geoHash, pickle.dumps(lowResMap))
  else:
    print("tile:", geoHash, " is NULL. delete", file=sys.stderr)
    r.delete(geoHash)


def updateLowResMapData(parentLowResMap, childTile, lat0, lng0, lats, lngs):
  # updateLowResMap用のサブルーチン
  # 子供が実データのときに、それを親に反映させる
  # registLrMap(addLowResMap) とほとんど同じルーチンなので共用化するべき
  for poidata in childTile:
    poi = poidata.decode().split(',', -1)  # decodeはredisから取ってきたデータなら必要だが・・
    lat = float(poi[0])
    lng = float(poi[1])
    del poi[0:2]
    addLowResMap("", lat, lng, poi, parentLowResMap, lat0, lng0, lats, lngs)


def updateLowResMapSub(parentLowResMap, childLowResMap, px0, py0):
  # updateLowResMap用のサブルーチン
  # 子供がlowResMapの時に、それを親に反映させる
  # 一つの子供のlowResMapを対象(親におけるその相対ピクセル位置px0,py0)
  # print("childLowResMap:",childLowResMap)
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
        if childPixData[i] != None:
          if dType == T_ENUM:  # 列挙型データの統計
            if parentPixData[i] == None:
              parentPixData[i] = {}
            for key, val in childPixData[i].items():
              if (key in parentPixData[i]):
                parentPixData[i][key] += val
              else:
                parentPixData[i][key] = val
          elif dType == T_NUMB:  # 数値型データの統計
            if parentPixData[i] == None:
              parentPixData[i] = []
              parentPixData[i].append(childPixData[i][0])
              parentPixData[i].append(childPixData[i][1])
            else:
              parentPixData[i][0] += childPixData[i][0]
              parentPixData[i][1] += childPixData[i][1]
          else:  # 文字列・・
            if parentPixData[i] == None:
              parentPixData[i] = childPixData[i]
            else:
              parentPixData[i] += childPixData[i]


def investigateKeys(registDataList, maxLevel):  # 高性能化の試行
  # そのPOIレコードに対応するoverflowしていないタイル番号をバースト探索する
  global overFlowKeys
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
    pipe = r.pipeline()

    query2redis = {}
    queryKeyList = []  # これは要らないのかもしれない

    for j in range(len(registDataList)):
      data = registDataList[j]
      lat = data["lat"]
      lng = data["lng"]
      if (keys[j] == "") or (keys[j][-1] != ":"):  # 見つかったもの":"付与はそれ以上深堀しない

        ans0, lat0s[j], lng0s[j], latss[j], lngss[j] = getGeoHashCode(lat, lng, lat0s[j], lng0s[j], latss[j], lngss[j])
        keys[j] += ans0
        if not (keys[j] in query2redis):
          pipe.type(keys[j])
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


def setDataList(registDataList, keys):
  pipe = r.pipeline()
  for j in range(len(keys)):
    key = keys[j]
    data = registDataList[j]["data"]
    if UseRedisHash:
      hkey = registDataList[j]["hkey"]
      pipe.hset(key, hkey,
                data)  # 緯度経度も含め全データをハッシュキーにしてたのは時として丸めが起きハッシュキーとして疑問ありなので、除外したものを"hkey"に入れる(これはgetOneDataで作ってる)
      # 緯度経度から生成されたgeoHashが1番目のredisキー、2番目のhashキーがその他のデータ(もしくはデータのID)　という形で検索できるのでそれで良いと考える
      # もしデータの緯度経度が変更されるようなデータ変更が起きる場合、データ変更前の緯度経度(もしくはgeoHash)をセットで送れば問題ないはず
    else:
      pipe.rpush(key, data)
  ans = pipe.execute()
  # print ("setDataList:",ans,file=sys.stderr)
  return (ans)


def checkSizes(keys):
  # 登録されたkeyに対するデータサイズを調査する　なんかちょっとロジックが無駄なことやってるかも？
  pipe = r.pipeline()
  keyList = []  # このへん 最初からdictで順番守られるんじゃないかな？
  keyDict = {}
  for key in keys:
    if key in keyDict:
      pass
    else:
      keyList.append(key)
      if UseRedisHash:
        pipe.hlen(key)
      else:
        pipe.llen(key)
      keyDict[key] = True
  sizes = pipe.execute()
  keyDict = {}  #そうすればこれ以下はやらなくても勝手にできてるのでは？
  for j in range(len(sizes)):
    keyDict[keyList[j]] = sizes[j]
  # print(sizes)
  return (keyDict)


def checkSiblingSizes(keys):
  # 登録されたkeyの兄弟全体のデータサイズを調査する(データ削除に伴うタイルの再統合チェック用)
  # 兄弟のうち一つでもlowResMap(redis string type)があったら、その時点でサイズオーバーとみなす
  # pipe = r.pipeline()
  # とりあえずパイプライン使わないで簡単実装してみる・・・
  keyDict = {}
  dataSize = 0
  for key in keys:
    if key in keyDict:
      pass
    else:
      pKey = key[0:-1]
      try:
        dataSizeA = r.hlen(pKey + "A")
        dataSizeB = r.hlen(pKey + "B")
        dataSizeC = r.hlen(pKey + "C")
        dataSizeD = r.hlen(pKey + "D")
        dataSize = dataSizeA + dataSizeB + dataSizeC + dataSizeD
      except:
        dataSize = listLimit + 1
      keyDict[key] = dataSize

  # print("checkSiblingSizes:",keyDict)
  return (keyDict)


def burstQuadPart(dataSizes):
  for key, count in dataSizes.items():
    if count >= listLimit:
      lat0, lng0, lats, lngs = geoHashToLatLng(key)
      quadPart(key, lat0, lng0, lats, lngs)


registDataList = []
burstSize = 600


def registData(oneData, maxLevel):
  global registDataList
  registDataList.append(oneData)
  if len(registDataList) < burstSize:
    pass  # debug... 2019/2/21
    psize = -1
  else:
    psize = burstRegistData(registDataList, maxLevel)
    registDataList = []
  return (psize)


def burstRegistData(registDataList, maxLevel):
  # 一括してデータの登録を行う
  keys = investigateKeys(registDataList, maxLevel)
  # print ("keys:",keys)
  # print (registDataList)
  ans = setDataList(registDataList, keys)
  dataSizes = checkSizes(keys)  # dataSizes: dict [geoHash:size]
  burstQuadPart(dataSizes)

  # print ("dataSizes:",dataSizes)
  return ({"success": len(ans), "keys": keys})


def flushRegistData(maxLevel):
  # バッファにたまっているデータをしっかり書き出してバッファもクリアする
  global registDataList
  psize = burstRegistData(registDataList, maxLevel)
  registDataList = []
  return (psize)


# データを１個追加する。 [[[ OBSOLUTED ]]]
# これを呼んだだけではLowResMapは更新されない。
# LowResMapの更新方法は２つ
# データ１個づつ更新させたい場合 updateAncestorsLowResMap
# 全部作り直す場合 buildAllLowResMap
def registOneData(oneData, maxLevel):
  lat = oneData.lat
  lng = oneData.lng
  data = oneData.data
  global overFlowKeys
  ans = ""  # geoHash
  lngs = 360.0
  lats = 360.0
  lng0 = -180.0
  lat0 = -180.0
  for i in range(maxLevel):
    # レベルを深めつつoverFlowしてないgeoHashタイルにストアする
    ans0, lat0, lng0, lats, lngs = getGeoHashCode(lat, lng, lat0, lng0, lats, lngs)
    # print(ans0,lats,lngs)
    ans += ans0
    if ans in overFlowKeys:
      # 下のオーバーフローキーの確認がかなり重たい rev2と比べてrev3が８倍ぐらい重いのを改善
      o = True
    else:  ############################ この辺　今作業中です！！！
      rType = r.type(ans)
      if rType == b"string":
        # overflowed lowResMap
        # overflowしたキーは、バイナリ（ストリング）データとして固有オブジェクト構造を保存している
        overFlowKeys[ans] = True
        print("add over flow keys: " + ans, file=sys.stderr)
      elif rType == b"list":
        ## realData
        tileDataSize = r.llen(ans)
        if tileDataSize < listLimit:
          # overflowしてないキーはlist構造で個々のPOIをCSV文字列で保存している
          #          r.rpush(ans, data)
          break
        else:
          # ちょうどオーバーフローしたところでは、下の階層を作ってまずは分割（ループは終わらないのでその次のループで実際に下の階層にデータをストアする
          quadPart(ans, lat0, lng0, lats, lngs)
      else:  # Noneすなわちキーが存在しない時は追加
        #        r.rpush(ans, data)
        break


#  print(".", end='')
#  print ("Regist: Key:", ans ," Val:",data)

deleteDataList = []


def deleteData(oneData, maxLevel):
  global deleteDataList
  deleteDataList.append(oneData)
  if len(deleteDataList) < burstSize:
    pass  # debug.. なんかハマった・・
    psize = -1
  else:
    psize = burstDeleteData(deleteDataList, maxLevel)
    deleteDataList = []
  return (psize)


def flushDeleteData(maxLevel):
  # バッファにたまっているデータ分も消去してバッファもクリアする
  global deleteDataList
  psize = burstDeleteData(deleteDataList, maxLevel)
  deleteDataList = []
  return (psize)


def burstDeleteData(deleteDatas, maxLevel):
  # 一括してデータの削除を行う
  keys = investigateKeys(deleteDatas, maxLevel)
  ans = delDataList(deleteDatas, keys)
  dataSizes = checkSiblingSizes(keys)  # dataSizes: dict [geoHash:size]
  print("burstDeleteData dataSizes:", dataSizes, file=sys.stderr)
  burstCombine(dataSizes)
  return ({"success": len(ans), "keys": keys})


def burstCombine(dataSizes):
  # 兄弟の合計がリミットを下回っていたら上のタイルに統合してしまう
  # ただしすべての兄弟が実データ（lowResMapでない）の場合に限る
  # pipe版
  # ISSUE: カラのタイルが残存してしまう場合がある　が、updateLowResMapで消せるはず
  combinedKeys = {}
  for key, count in dataSizes.items():
    if count < listLimit:
      pKey = key[0:-1]
      if pKey in combinedKeys:
        pass
      else:
        pipe = r.pipeline()
        combinedKeys[pKey] = True
        print("burstCombine:", key, "->", pKey, file=sys.stderr)
        pipe.hgetall(pKey + "A")
        pipe.hgetall(pKey + "B")
        pipe.hgetall(pKey + "C")
        pipe.hgetall(pKey + "D")
        pipe.delete(pKey + "A")
        pipe.delete(pKey + "B")
        pipe.delete(pKey + "C")
        pipe.delete(pKey + "D")
        pipe.delete(pKey)
        dats = pipe.execute()
        dat = dats[0]
        dat.update(dats[1])
        dat.update(dats[2])
        dat.update(dats[3])
        if len(dat) > 0:
          r.hmset(pKey, dat)


def delDataList(deleteDatas, geoHashKeys):
  pipe = r.pipeline()
  for j in range(len(geoHashKeys)):
    geoHashKey = geoHashKeys[j]
    hkey = deleteDatas[j]["hkey"]
    if UseRedisHash:
      print("hdel", geoHashKey, hkey)
      pipe.hdel(geoHashKey, hkey)
    else:
      pass
  ans = pipe.execute()
  print("delDataList pipe:::", ans, deleteDatas, geoHashKeys, file=sys.stderr)
  return (ans)


def saveAllSvgMap(lowResImage=False):
  global r
  bkeys = r.keys("[A-D]*")
  pipe = r.pipeline()
  for key in bkeys:
    pipe.type(key)
  types = pipe.execute()

  for j in range(len(bkeys)):
    key = bkeys[j].decode()
    dtype = types[j]
    # key = key.decode()
    saveSvgMapTileN(key, dtype, lowResImage)
    #    saveSvgMapTile(key, dtype) # 性能が悪いのでobsolute・・・
    if (j % 20 == 0):
      print(100 * (j / len(bkeys)), "%", file=sys.stderr)


def saveSvgMapTile(geoHash, dtype=None):  # pythonのXML遅くて足かせになったので、この関数は使わなくしました 2019/1/16
  global r, csvSchema, csvSchemaType
  if dtype == None:
    dtype = r.type(geoHash)
  thisZoom = len(geoHash)
  doc, svg = csv2svgmap.create_svgMapDoc()
  svg.setAttribute("property", ",".join(csvSchema))
  lat0, lng0, lats, lngs = geoHashToLatLng(geoHash)

  if dtype == b"string":  # そのタイルはオーバーフローしている実データがないlowRes pickleタイル
    thisTile = pickle.loads(r.get(geoHash))
    thisG = doc.createElement("g")  # 下のlowResPOI(rect)を入れる
    childG = doc.createElement("g")  # childSVGのanimation要素を入れる
    pixW = 100 * lngs / lowresMapSize
    pixH = 100 * lats / lowresMapSize
    for xyKey, data in thisTile.items():
      xy = xyKey.split("_")
      if (len(xy) == 2):
        x = int(xy[0])
        y = int(xy[1])
        lng = lng0 + lngs * (x / lowresMapSize)
        lat = lat0 + lats * (y / lowresMapSize)
        title = xyKey
        rect = doc.createElement("rect")
        rect.setAttribute("x", "{:.3f}".format(100 * lng))
        rect.setAttribute("y", "{:.3f}".format(-100 * lat - pixH))  # 緯度・pixHの足し方ちょっと怪しい・・・
        rect.setAttribute("width", "{:.3f}".format(pixW))
        rect.setAttribute("height", "{:.3f}".format(pixH))
        rect.setAttribute("content", "totalPois:" + str(data[len(csvSchemaType)]))
        thisG.appendChild(rect)
        """
        use = doc.createElement("use")
        use.setAttribute("xlink:href", "#p0")
        use.setAttribute("transform", 'ref(svg,{:.3f},{:.3f})'.format(100*lng, -100*lat))
        use.setAttribute("xlink:title", title)
        use.setAttribute("x", "0")
        use.setAttribute("y", "0")
        meta ="TBD"
        use.setAttribute("content",meta)
        thisG.appendChild(use)
        """
    thisG.setAttribute("fill", "blue")
    thisG.setAttribute("visibleMaxZoom", str(topVisibleMinZoom * pow(2, thisZoom - 1)))
    svg.appendChild(thisG)

    pipe = r.pipeline()  # パイプ使って少し高速化？
    pipe.exists(geoHash + "A")
    pipe.exists(geoHash + "B")
    pipe.exists(geoHash + "C")
    pipe.exists(geoHash + "D")
    ceFlg = pipe.execute()

    for i, exs in enumerate(ceFlg):  # link to child tiles
      cN = chr(65 + i)
      childGeoHash = geoHash + cN
      #      print("EXISTS?", cN,exs)
      if (exs):
        anim = doc.createElement("animation")
        anim.setAttribute("xlink:href", svgFileNameHd + childGeoHash + ".svg")
        lat_shift = 0
        lng_shift = 0
        if cN == "B":
          lng_shift = lngs / 2
        elif cN == "C":
          lat_shift = lats / 2
        elif cN == "D":
          lng_shift = lngs / 2
          lat_shift = lats / 2

        anim.setAttribute("x", "{:.3f}".format(100 * (lng0 + lng_shift)))
        anim.setAttribute("y", "{:.3f}".format(-100 * (lat0 + lat_shift + lats / 2)))  # 緯度・lats/2の足し方ちょっと怪しい・・・
        anim.setAttribute("width", "{:.3f}".format(100 * lngs / 2))
        anim.setAttribute("height", "{:.3f}".format(100 * lats / 2))
        childG.appendChild(anim)
    childG.setAttribute("visibleMinZoom", str(topVisibleMinZoom * pow(2, thisZoom - 1)))
    svg.appendChild(childG)

  else:  # 実データ
    if dtype == b"list":
      src = r.lrange(geoHash, 0, -1)  # 全POI取得
    else:
      src = r.hgetall(geoHash)
      src = list(src.values())
    print(geoHash, "readData:len", len(src), file=sys.stderr)
    for poidata in src:
      poi = poidata.decode().split(',', -1)
      lat = float(poi[0])
      lng = float(poi[1])
      del poi[0:2]
      title = poi[0]
      use = doc.createElement("use")
      use.setAttribute("xlink:href", "#p0")
      use.setAttribute("transform", 'ref(svg,{:.3f},{:.3f})'.format(100 * lng, -100 * lat))
      use.setAttribute("xlink:title", title)
      use.setAttribute("x", "0")
      use.setAttribute("y", "0")
      use.setAttribute("content", ",".join(poi))
      svg.appendChild(use)

  csv2svgmap.save_xmldoc(doc, svgFileNameHd + geoHash + ".svg")


def saveSvgMapTileN(
    geoHash=None,  # タイルハッシュコード
    dtype=None,  # あらかじめわかっている場合のデータタイプ(低解像タイルか実データタイル化がわかる)
    lowResImage=False,  # 低解像タイルをビットイメージにする場合
    onMemoryOutput=False,  # ライブラリとして使用し、データをオンメモリで渡す場合
    returnBitImage=False):  # オンメモリ渡し(上がTrue限定)のとき、低解像ビットイメージデータを要求する場合
  # saveSvgMapTileを置き換え、SVGMapコンテンツをSAX的に直生成することで高速化を図る　確かに全然早くなった。pythonってやっぱりゆる系？・・・ 2019/1/16
  outStrL = []  # 出力するファイルの文字列のリスト　最後にjoinの上writeする

  global r, csvSchema, csvSchemaType
  #    print(dtype)

  if geoHash == None or geoHash == "":  # レベル0のタイルをgeoHash=Noneで作るようにした2019/2/26
    dtype = b"string"
    lat0 = -180
    lng0 = -180
    lats = 360
    lngs = 360
    geoHash = ""
  else:
    lat0, lng0, lats, lngs = geoHashToLatLng(geoHash)
    thisZoom = len(geoHash)

  if dtype == None:
    dtype = r.type(geoHash)
  outStrL.append("<?xml version='1.0' encoding='UTF-8'?>\n<svg property='")
  outStrL.append(",".join(csvSchema))
  outStrL.append(
      "' viewBox='9000,-5500,10000,10000' xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink'>\n"
  )
  outStrL.append("<globalCoordinateSystem srsName='http://purl.org/crs/84' transform='matrix(100,0,0,-100,0,0)'/>\n")

  if dtype == b"string":  # そのタイルはオーバーフローしている実データがないlowRes pickleタイル
    if lats < 360:  # レベル0のタイル(レイヤールートコンテナ)じゃない場合はそのレベルの低解像度タイルを入れる
      pixW = 100 * lngs / lowresMapSize
      pixH = 100 * lats / lowresMapSize
      outStrL.append("<g fill='blue' visibleMaxZoom='{:.3f}'>\n".format((topVisibleMinZoom * pow(2, thisZoom - 1))))

      # bitImage出力 http://d.hatena.ne.jp/white_wheels/20100322/p1
      if lowResImage:
        if onMemoryOutput and not returnBitImage:
          pass  # ただし、オンメモリ生成でビットイメージ要求がない場合はビットイメージ生成必要ない
        else:
          bitImage = np.zeros([lowresMapSize, lowresMapSize, 4], dtype=np.uint8)
          bitImage[:, :] = [0, 0, 0, 0]  # black totally transparent

      if lowResImage and (onMemoryOutput and not returnBitImage):
        pass  # オンメモリ生成でビットイメージ要求がない場合、しかもlowResImageの場合は低分解能コンテンツ生成の必要はない(コンテナ作るだけ)
      else:
        thisTile = pickle.loads(r.get(geoHash))
        # print(geoHash, "lowRes Data:len", len(thisTile), thisTile)
        # print(geoHash, "lowRes Data:len", len(thisTile))
        for xyKey, data in thisTile.items():
          xy = xyKey.split("_")
          if (len(xy) == 2):
            x = int(xy[0])
            y = int(xy[1])
            # print(x,y,xyKey,data)
            if (lowResImage):
              yi = lowresMapSize - y - 1
              bitImage[yi, x, 2] = 255  # blue=255
              bitImage[yi, x, 3] = 255  # alpha=255

            else:
              lng = lng0 + lngs * (x / lowresMapSize)
              lat = lat0 + lats * (y / lowresMapSize)
              title = xyKey
              outStrL.append(' <rect x="')
              outStrL.append('{:.3f}'.format(100 * lng))
              outStrL.append('" y="')
              outStrL.append('{:.3f}'.format(-100 * lat - pixH))
              outStrL.append('" width="')
              outStrL.append('{:.3f}'.format(pixW))
              outStrL.append('" height="')
              outStrL.append('{:.3f}'.format(pixH))
              outStrL.append('" content="totalPois:')
              outStrL.append(str(data[len(csvSchemaType)]))
              outStrL.append('" />\n')

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
            img.save(targetDir + svgFileNameHd + geoHash + ".png")
        outStrL.append(" <image style='image-rendering:pixelated' xlink:href='")
        outStrL.append(svgFileNameHd + geoHash + ".png")
        outStrL.append("' x='{:.3f}".format(100 * (lng0)))
        outStrL.append("' y='{:.3f}".format(-100 * (lat0 + lats)))
        outStrL.append("' width='{:.3f}".format(100 * lngs))
        outStrL.append("' height='{:.3f}'/>\n".format(100 * lats))

    pipe = r.pipeline()  # パイプ使って少し高速化できたか？
    pipe.exists(geoHash + "A")
    pipe.exists(geoHash + "B")
    pipe.exists(geoHash + "C")
    pipe.exists(geoHash + "D")
    ceFlg = pipe.execute()

    if lats < 360:
      outStrL.append("</g>\n<g fill='blue' visibleMinZoom='{:.3f}'>\n".format(
          (topVisibleMinZoom * pow(2, thisZoom - 1))))
    else:  # レベル0のレイヤルートコンテナの場合
      outStrL.append(
          "<defs>\n <g id='p0'>\n  <image height='27' preserveAspectRatio='none' width='19' x='-8' xlink:href='mappin.png' y='-25'/>\n </g>\n</defs>\n"
      )
      outStrL.append("<g>\n")

    for i, exs in enumerate(ceFlg):  # link to child tiles
      cN = chr(65 + i)
      childGeoHash = geoHash + cN
      #      print("EXISTS?", cN,exs)
      if (exs):
        outStrL.append(" <animation xlink:href='")
        outStrL.append(svgFileNameHd + childGeoHash + ".svg")
        lat_shift = 0
        lng_shift = 0
        if cN == "B":
          lng_shift = lngs / 2
        elif cN == "C":
          lat_shift = lats / 2
        elif cN == "D":
          lng_shift = lngs / 2
          lat_shift = lats / 2

        outStrL.append("' x='{:.3f}".format(100 * (lng0 + lng_shift)))
        outStrL.append("' y='{:.3f}".format(-100 * (lat0 + lat_shift + lats / 2)))
        # 緯度・lats/2の足し方ちょっと怪しい・・・
        outStrL.append("' width='{:.3f}".format(100 * lngs / 2))
        outStrL.append("' height='{:.3f}'/>\n".format(100 * lats / 2))
    outStrL.append("</g>\n")

  else:  # 実データ
    outStrL.append(
        "<defs>\n <g id='p0'>\n  <image height='27' preserveAspectRatio='none' width='19' x='-8' xlink:href='mappin.png' y='-25'/>\n </g>\n</defs>\n"
    )

    if dtype == b"list":
      src = r.lrange(geoHash, 0, -1)  # 全POI取得
    else:
      src = r.hgetall(geoHash)
      src = list(src.values())

    print(geoHash, "real Data:len", len(src), file=sys.stderr)
    for poidata in src:
      poi = poidata.decode().split(',', -1)
      lat = float(poi[0])
      lng = float(poi[1])
      del poi[0:2]
      title = poi[0]
      outStrL.append(" <use xlink:href='#p0' transform='ref(svg,{:.3f},{:.3f})'".format(100 * lng, -100 * lat))
      outStrL.append(" xlink:title='")
      outStrL.append(title)
      outStrL.append("' x='0' y='0' content='")
      outStrL.append(xmlEscape(",".join(poi)))
      outStrL.append("'/>\n")
  outStrL.append("</svg>\n")

  if (onMemoryOutput):  # 文字列として返却するだけのオプション
    return "".join(outStrL)
  else:
    with open(targetDir + svgFileNameHd + geoHash + ".svg", mode='w', encoding='utf-8') as f:
      f.write("".join(outStrL))  # writeは遅いらしいので一発で書き出すようにするよ
      # f.flush() # ひとまずファイルの書き出しはシステムお任せにしましょう・・


def xmlEscape(str):
  ans = str.replace("'", "&apos;")
  ans = ans.replace('"', "&quot;")
  ans = ans.replace("&", "&amp;")
  return (ans)


append = "None"


def init():
  global r, csvSchema, csvSchemaType, latCol, lngCol
  r = redis.Redis(host='localhost', port=6379, db=0)
  if len(csvSchema) == 0:
    if r.exists(b"schema"):
      csvSchemaObj = pickle.loads(r.get(b"schema"))
      # print("load csvSchemaObj:",csvSchemaObj)
      csvSchema = csvSchemaObj.get("schema")
      csvSchemaType = csvSchemaObj.get("type")
      latCol = csvSchemaObj.get("latCol")
      lngCol = csvSchemaObj.get("lngCol")
  else:
    pass
    # print("SKIP load schema",file=sys.stderr)


def getSchema(header):
  csvSchema = []
  csvSchemaType = []
  latCol = -1
  lngCol = -1
  titleCol = -1
  # http://programming-study.com/technology/python-for-index/
  # 最初の行はカラム名とその型（スキーマを獲得する）（ToDo: サブルーチン化）
  for i, hdname in enumerate(header):
    if hdname.find("東経") >= 0 or hdname.lower().find("longitude") >= 0 or hdname.find("経度") >= 0:
      lngCol = i
    elif hdname.find("北緯") >= 0 or hdname.lower().find("latitude") >= 0 or hdname.find("緯度") >= 0:
      latCol = i
    else:
      csvSchema.append(hdname)
      if hdname.lower().endswith(":e"):
        csvSchemaType.append(T_ENUM)
      elif hdname.lower().endswith(":s"):
        csvSchemaType.append(T_STR)
      elif hdname.lower().endswith(":n"):
        csvSchemaType.append(T_NUMB)
      else:
        csvSchemaType.append(T_STR)

  return csvSchema, csvSchemaType, latCol, lngCol, titleCol


def getCsvReader(inputcsv):
  csv_file = open(inputcsv, "r", encoding="utf-8", errors="", newline="")
  # リスト形式
  file = csv.reader(
      csv_file, delimiter=",", doublequote=True, lineterminator="\r\n", quotechar='"', skipinitialspace=True)
  return file


def readAndRegistData(file, latCol, lngCol, maxLevel):
  global r, csvSchema, csvSchemaType
  # 二行目以降がデータになる～実データを読み込む（ToDo: サブルーチン化）
  lines = 0
  for row in file:
    # rowはList
    # row[0]で必要な項目を取得することができる
    #     print(row)
    oneData = getOneData(row, latCol, lngCol)
    registData(oneData, maxLevel)
    lines = lines + 1
    if (lines % 1000 == 0):
      print(lines, file=sys.stderr)

  flushRegistData(maxLevel)  # バッファを全部書き出す


def readAndDeleteData(file, latCol, lngCol, maxLevel):
  global r, csvSchema, csvSchemaType
  # 二行目以降がデータになる～実データを読み込む（ToDo: サブルーチン化）
  lines = 0
  for row in file:
    # rowはList
    # row[0]で必要な項目を取得することができる
    #     print(row)
    oneData = getOneData(row, latCol, lngCol)
    deleteData(oneData, maxLevel)
    lines = lines + 1
    if (lines % 1000 == 0):
      print(lines, file=sys.stderr)

  flushDeleteData(maxLevel)  # バッファを全部消去処理して完了させる


def getOneData(row, latCol, lngCol):
  lat = float(row[latCol])
  lng = float(row[lngCol])
  meta = ""
  mi = 0
  for i, data in enumerate(row):
    if (i != latCol and i != lngCol):
      meta += data
      if mi < len(csvSchema) - 1:
        meta += ","
      mi = mi + 1
  # print("ParsedCsvData:",lat,lng,meta)
  oneData = {
      "lat": lat,
      "lng": lng,
      "data": row[latCol] + "," + row[lngCol] + "," + meta,
      "hkey": meta
  }  # hkeyで実データのハッシュを直に指定 2019/3/13
  return (oneData)


def main():
  global r, csvSchema, csvSchemaType, targetDir, lowResIsBitImage, latCol, lngCol

  init()
  parser = argparse.ArgumentParser()
  parser.add_argument("--input")
  parser.add_argument("--append")
  parser.add_argument("--dir")
  parser.add_argument("--lowres")
  parser.add_argument("--delete")
  parser.add_argument("--onlysaveallmap", action='store_true')
  parser.add_argument("--onlybuildlowres", action='store_true')
  parser.add_argument("--debug", action='store_true')

  inputcsv = "./worldcitiespop_jp.csv"
  args = parser.parse_args()
  newcsv = args.input
  appendcsv = args.append
  deletecsv = args.delete

  appendMode = False
  deleteMode = False

  if (args.dir):
    targetDir = args.dir

  if (targetDir != "" and targetDir[-1:] != "/"):
    targetDir = targetDir + "/"

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
    saveSvgMapTileN("BDDD", None, True)
    return

  if (args.onlysaveallmap):
    saveAllSvgMap(lowResIsBitImage)
    return

  if (args.onlybuildlowres):
    buildAllLowResMap()
    return

  if (not appendMode and not deleteMode):
    print("FlushDB", file=sys.stderr)
    r.flushdb()  # DBを消去する

  # csvファイルリーダーを作る
  file = getCsvReader(inputcsv)

  header = next(file)
  # ヘッダからスキーマを構築
  csvSchema, csvSchemaType, latCol, lngCol, titleCol = getSchema(header)

  if (appendMode or deleteMode):
    pass
  else:
    # スキーマレコードは必ず新しく作った時だけに生成するようにしておく　ねんのため
    csvSchemaObj = {"schema": csvSchema, "type": csvSchemaType, "latCol": latCol, "lngCol": lngCol}
    print("csvSchemaObj:", csvSchemaObj, file=sys.stderr)
    r.set(b"schema", pickle.dumps(csvSchemaObj))

  print("latCol,lngCol,csvSchema, csvSchemaType:", latCol, lngCol, csvSchema, csvSchemaType, file=sys.stderr)

  # CSVファイルを読み込みデータをredisに登録する
  if (deleteMode):
    readAndDeleteData(file, latCol, lngCol, maxLevel)
  else:
    readAndRegistData(file, latCol, lngCol, maxLevel)

  # 低解像度タイルデータベースを全部一から生成する
  buildAllLowResMap()

  # printAllLowResMap()

  # SVGMapコンテンツ(オプションによって低解像ビットイメージ)を全部一から生成する
  saveAllSvgMap(lowResIsBitImage)

  print("schema:", pickle.loads(r.get(b"schema")), file=sys.stderr)


if __name__ == "__main__":
  main()
