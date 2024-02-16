import sys
import os
import abc
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.svgmapContainer import SvgmapContainer, Tag


class DataGeneratorIF(metaclass=abc.ABCMeta):
  @abc.abstractmethod
  def regist_defs() -> None:
    raise NotImplementedError()
  
  @abc.abstractmethod
  def outputSvgContent() -> str:
    raise NotImplementedError()

'''
デフォルトアイコンをユーザがUploadした画像が使えるようにする機能

現在POIのアイコンしか切り替えることができないため、
本当はsvgmapContainerをラップし、POI以外にもラスターデータも切り替えることが
できる機能を具備したい

完全にカスタムなコンテンツ生成ルーチンを組み込むI/F
customDataGeneratorオブジェクトは、outputSvgContent([poiObj],svgc:SvgmapContainer):str(xmlStr)
  poiObj(Dict) = {"lat", "lng", "title", "metadata"}
  regist_defs(svgc:SvgmapContainer)
を実装している必要がある
'''
class DefaultDataGenerator(DataGeneratorIF):
  def __init__(self, filepath=""):
    self.poi_size = ["-8", "-25", "19", "27"]  # x,y,width,height
    self.poi_color = [{"flag": "f1", "color": "mappin.png"}]
    self.fallbackIcons = ["pin_blue.png", "pin_cyan.png", "pin_green.png", "pin_yellow.png", "pin_orange.png", "pin_pink.png", "pin_red.png", "pin_purple.png", "pin_red_cross.png", "pin_green_cross.png", "pin_blue_cross.png", "pin_pink_cross.png"] # defaultIconPathがなく、defaultIconがあった場合のフォールバック

  def regist_defs(self, svgc, schema):  # カスタムのアイコン定義を可能にする。ただし、webApp:redisDatasetBuilder.htmlでschemaに新設した'defaultIconPath'を使っている
    if "defaultIconPath" in schema:
      self.poi_color[0]["color"] = schema["defaultIconPath"]
    elif "defaultIcon" in schema: #defaultIconPathがなく、defaultIconで番号指定されているだけのケースへのフォールバック
      self.poi_color[0]["color"] = self.fallbackIcons[int(schema["defaultIcon"])]

    svgc.regist_size(self.poi_size)
    svgc.regist_defs(self.poi_color)
    svgc.color_column_index = None
    
  def outputSvgContent(self, outPoiL, svgc, schema):

    for poi in outPoiL:
      # poi:  {"lat": lat, "lng": lng, "title": title, "metadata": metadata}
      svgc.add_content(poi["title"], poi["lat"], poi["lng"], poi["metadata"])

    return (svgc.output_str_to_container())
