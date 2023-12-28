import unittest
import os
import io
import json
import sys
from unittest import mock
from scripts.lib.svgmapContainer import Tag, SvgmapContainer

class TestOfTagClass(unittest.TestCase):
  def test_normal(self):
    self.tag = Tag("g")
    for itm in ["img"]:
      c = Tag(itm)
      c.x = "0"
      c.y = "0"
      c.width = "5"
      c.height = "5"
      c.__setattr__("xlink:href", "aaa.png")  # :が入るメタデータについては__setattr__で記載する
      self.tag.append_child(c)

    self.assertEqual(self.tag.output_str(), '<g>\n<img x="0" y="0" width="5" height="5" xlink:href="aaa.png"/>\n</g>')

class TestOfSvgmapContainerClass(unittest.TestCase):
  def setUp(self):
    self.svgc = SvgmapContainer(["id", "name", "area", "address", "flg"], ["str", "str", "str", "str", "str"])

  def test_normal(self):
    poiSize = ["0", "0", "5", "5"]  # poiSize = [x, y, width, heigh]
    poiColor = [
        {
            "flag": "f1",
            "color": "#FF0000"
        },
        {
            "flag": "f2",
            "color": "#FFFF00"
        }
    ]  # poiColor = f1:#FF0000,f2:#FFFF00 -> split(",") -> split(":")
    colorColumn = 4

    self.svgc.regist_size(poiSize)
    self.svgc.regist_defs(poiColor)
    self.svgc.color_column_index = colorColumn
    self.assertEqual(self.svgc.defs, '<defs>\n<g id="f1">\n<rect fill="#FF0000" x="0" y="0" width="5" height="5"/>\n</g>\n<g id="f2">\n<rect fill="#FFFF00" x="0" y="0" width="5" height="5"/>\n</g>\n</defs>\n')
    self.assertEqual(self.svgc.color_column_index, 4)
  
  def test_addContents(self):
    correct = "<?xml version='1.0' encoding='UTF-8'?>\n<svg property='id,name,area,address,flg' data-property-type='str,str,str,str,str' xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink' viewBox='9000.000,-5500.000,10000.000,10000.000' >\n<globalCoordinateSystem srsName='http://purl.org/crs/84' transform='matrix(100,0,0,-100,0,0)'/>\n  <use xlink:href='#' xlink:title='poi1' transform='ref(svg,13400.0,-3500.0)' content='0,poi1,tokyo,東京都新宿区,f2'  x='0' y='0'/>\n</svg>"
    self.svgc.add_content("poi1", "35.000", "134", ["0", "poi1", "tokyo", "東京都新宿区", "f2"])
    
    self.assertEqual(self.svgc.output_str_to_container(), correct)
    self.svgc.save_to_container_file("./tests/temporary/sample.svg")
    with open("./tests/temporary/sample.svg", "r") as f:
      data = f.read() 
    self.assertEqual(data, correct)