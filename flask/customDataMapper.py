import sys, os
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

  def regist_defs(self, svgc):
    svgc.regist_size(self.poiSize)
    svgc.color_column_index = None
    svgc.regist_defs(self.poiColor)

  def outputSvgContent(self, outPoiL, svgc):

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

  def regist_defs(self, svgc):  # circleで
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

  def outputSvgContent(self, outPoiL, svgc):

    for poi in outPoiL:
      # poi:  {"lat": lat, "lng": lng, "title": title, "metadata": metadata}
      svgc.add_content(poi["title"], poi["lat"], poi["lng"], poi["metadata"])

    return (svgc.output_str_to_container())
