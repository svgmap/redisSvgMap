import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.svgmapContainer import SvgmapContainer, Tag


class DataGenerator(object):

  def __init__(self):
    self.name = "test"
    self.poiSize = ["0", "0", "5", "5"]  # poiSize = [x, y, width, heigh]
    self.poiColor = [
        {
            "flag": "f1",
            "color": "#FF0000"
        },
        {
            "flag": "f2",
            "color": "#FFFF00"
        },
        {
            "flag": "f3",
            "color": "#0000FF"
        },
    ]  # poiColor = f1:#FF0000,f2:#FFFF00,f3:#0000FF -> split(",") -> split(":")
    # self.colorColumn = 4

  def regist_defs(self, svgc, schema):
    svgc.regist_size(self.poiSize)
    svgc.color_column_index = None
    svgc.regist_defs(self.poiColor)

  def outputSvgContent(self, outPoiL, svgc, schema):

    for poi in outPoiL:
      # poi:  {"lat": lat, "lng": lng, "title": title, "metadata": metadata}
      svgc.add_content(poi["title"], poi["lat"], poi["lng"], poi["metadata"])

    return (svgc.output_str_to_container())


# カスタマイズしたdefsを作るパターン
# ただし、POIそのものはsvgmapContainerライブラリの組み込みのuse生成ルーチンを使う
class DataGenerator2(object):

  def __init__(self):
    self.poiSize = ["0", "0", "8"]  # poiSize = [cx, cy, r]
    self.poiColor = [
        {
            "flag": "f1",
            "color": "#00DD00"
        },
        {
            "flag": "f2",
            "color": "#FFFF00"
        },
        {
            "flag": "f3",
            "color": "#0000FF"
        },
    ]  # poiColor = f1:#FF0000,f2:#FFFF00,f3:#0000FF -> split(",") -> split(":")
    # self.colorColumn = 4

  def regist_defs(self, svgc, schema):  # circleで
    firstIcon = True
    defs = Tag("defs")
    for color in self.poiColor:
      # print(color)
      g = Tag("g")
      g.id = color["flag"]
      if (firstIcon):
        svgc.defaultIconId = g.id
        # print("firstIconID is ...............", svgc.defaultIconId)
        firstIcon = False

      tag = Tag("circle")
      tag.fill = color["color"]
      tag.cx, tag.cy, tag.r = self.poiSize
      g.append_child(tag)
      defs.append_child(g)

    svgc.add_tag(defs)

  def outputSvgContent(self, outPoiL, svgc, schema):

    for poi in outPoiL:
      # poi:  {"lat": lat, "lng": lng, "title": title, "metadata": metadata}
      svgc.add_content(poi["title"], poi["lat"], poi["lng"], poi["metadata"])

    return (svgc.output_str_to_container())

# カスタマイズしたdefsを作るパターン2 2022/08/05
# スキーマで指定されているアイコンを使用するicon定義defsを生成する
# csv2redis16.1とほぼ互換の機能を提供する


class DataGenerator3(object):

  def __init__(self):
    self.poi_size = ["-8", "-25", "19", "27"]  # x,y,width,height
    self.poi_color = [{"flag": "f1", "color": "mappin.png"}]

  def regist_defs(self, svgc, schema):  # カスタムのアイコン定義を可能にする。ただし、webApp:redisDatasetBuilder.htmlでschemaに新設した'defaultIconPath'を使っている
    if "defaultIconPath" in schema:
      self.poi_color[0]["color"] = schema["defaultIconPath"]
    svgc.regist_size(self.poi_size)
    svgc.regist_defs(self.poi_color)
    svgc.color_column_index = None

  def outputSvgContent(self, outPoiL, svgc, schema):

    for poi in outPoiL:
      # poi:  {"lat": lat, "lng": lng, "title": title, "metadata": metadata}
      svgc.add_content(poi["title"], poi["lat"], poi["lng"], poi["metadata"])

    return (svgc.output_str_to_container())
