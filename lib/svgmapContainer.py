#!/bin/python3
# -*- coding=utf-8 -*-
"""
SVGMapのコンテナファイルを生成するクラス

  ※CSV2redisの実装に則りpython標準のXMLパーサを使用するのではなく単純に文字列としてファイルに書き出すこととする
"""

from xml.sax.saxutils import escape
import math


class SvgmapContainer:
    def __init__(self, data_property: list, data_property_type: list):
        """
        svgmap Container
            __data_property_name:list
            __data_property_type:list
            __color_column_index:int
        """

        self.__output = []
        self.__header = []
        self.__footer = []
        self.__container = []
        self.__groups = {}
        self.__poi_size = []

        self.__definitions = []
        self.__data_property_name = data_property  # CSVカラム名
        self.__data_property_type = data_property_type  # カラムの型
        self.__color_column_index = len(self.__data_property_name) - 1  # 色を変更するカラム番号

        self.__header.append("<?xml version='1.0' encoding='UTF-8'?>\n<svg property='")
        self.__header.append(",".join(self.__data_property_name))
        # self.__header.append("' data-property-type='") # これいるんでしたっけ？
        # self.__header.append(",".join(self.__data_property_type))
        self.__header.append(
            "' viewBox='9000,-5500,10000,10000' xmlns='http://www.w3.org/2000/svg' xmlns:xlink='http://www.w3.org/1999/xlink'>\n"
        )
        self.__header.append(
            "<globalCoordinateSystem srsName='http://purl.org/crs/84' transform='matrix(100,0,0,-100,0,0)'/>\n"
        )

        self.__footer.append("</svg>")

    # defs tag @ svg
    @property
    def defs(self) -> dict:
        out_str = "<defs>\n"
        for d in self.__definitions:
            out_str += "  " + d.output_str()
        out_str += "</defs>\n"
        return out_str

    # イメージサイズの登録
    def regist_size(self, _size: list):
        """
        _size : [x y width height]
        """
        self.__poi_size = _size

    # 色フラグの登録
    def regist_flag(self, _colors: list):
        for color in _colors:
            tag = Tag(color["flg"], color["color"])
            tag.set_size(*self.__poi_size)
            self.__definitions.append(tag)

    # 全データ削除
    def clear(self) -> None:
        pass

    # 色を変更するカラム番号を取得
    @property
    def color_column_index(self) -> int:
        return self.__color_column_index

    # 色を変更するカラム番号を設定
    @color_column_index.setter
    def color_column_index(self, index: int) -> None:
        self.__color_column_index = index

    # SVG用のdefsタグへdictから変換
    def convert_defs_to_svgmap(self) -> str:
        pass

    # get all poi
    @property
    def contents(self) -> list:
        return self.__container

    # regist a poi
    def add_content(self, title, lat, lng, metadatas: list) -> None:
        """
        POIを登録する関数
            Input
                metadatas:メタデータが入っているリスト
        """
        if self.__color_column_index is None:
            # デフォルトの色を使用してプロット（既存と同じ青でいいかなぁ）
            pass
        else:
            #
            print(self.__data_property_type)
            if self.__data_property_type[self.__color_column_index] is str:
                self.__container.append(
                    {"title": title, "lat": lat, "lng": lng, "metadatas": metadatas}
                )
                pass
            #
            if self.__data_property_type[self.__color_column_index] is int:
                pass
            #
            if self.__data_property_type[self.__color_column_index] is float:
                pass

    def convert_raw_to_svgmap_tag(self) -> str:
        content = ""
        for item in self.__container:
            content += (
                "  <use xlink:href='#%s' xlink:title='%s' transform='ref(svg,%s,%s)' content='%s'/>\n"
                % (
                    item["metadatas"][self.__color_column_index],
                    escape(item["title"]),
                    100 * math.floor(float(item["lng"]) * 1000000) / 1000000,
                    -100 * math.floor(float(item["lat"]) * 1000000) / 1000000,
                    escape(",".join(item["metadatas"])),
                )
            )
        return content

    #
    def save_to_container_file(self, file_path):
        """
        ファイル出力
            input
                file_path:出力先のファイルパス
            return
                なし
        """
        with open(file_path, mode="w", encoding="utf-8") as f:
            f.write(self.output_str_to_container())

    def output_str_to_container(self):
        """
        SvgmapContainerファイルの文字列出力
        """
        # clear
        str_output = ""
        # header
        str_output += "".join(self.__header)

        str_output += self.defs
        # adding contents
        str_output += self.convert_raw_to_svgmap_tag()
        # footer
        str_output += "".join(self.__footer)
        return str_output


class Tag:
    def __init__(self, _id: str, _color: str):
        self.__id = _id  #
        self.__tag = ""  # tag name ex.) {image | rect | circle}
        self.__x = 0
        self.__y = 0
        self.__width = 0.3  # default : 0.3
        self.__height = 0.3  # default : 0.3
        self.__href = ""
        self.__title = ""
        self.__content = ""
        self.__fill = "#0000FF"  # default : #0000FF (Blue)
        self.__opacity = 0.7  # default : 0.7
        # check color code
        if "#" in _color:
            self.__tag = "rect"
            self.__fill = _color
        else:
            self.__tag = "image"
            self.__href = _color

    def set_size(self, _x, _y, _w, _h):
        self.__x = _x
        self.__y = _y
        self.__width = _w
        self.__height = _h

    def output_str(self) -> str:
        str_out = '<g id="%s" opacity="%s">' % (self.__id, str(self.__opacity))

        str_out += "<%s x='%s' y='%s' width='%s' height='%s' " % (
            self.__tag,
            self.__x,
            self.__y,
            self.__width,
            self.__height,
        )
        if self.__tag == "rect":
            str_out += "fill='%s'" % (self.__fill)
        else:
            str_out += "xlink:href='%s'" % (self.__href)

        str_out += "/></g>\n"
        return str_out


if __name__ == "__main__":

    svgc = SvgmapContainer(
        ["id", "name", "area", "address", "flg"], [str, str, str, str, str]
    )

    poiSize = ["0", "0", "1", "1"]  # poiSize = [x, y, width, heigh]
    poiColor = [
        {"flg": "f1", "color": "#FF0000"},
        {"flg": "f2", "color": "#FFFF00"},
        {"flg": "f3", "color": "#0000FF"},
    ]  # poiColor = f1:#FF0000,f2:#FFFF00,f3:#0000FF -> split(",") -> split(":")
    colorColumn = 4

    svgc.regist_size(poiSize)
    svgc.regist_flag(poiColor)
    svgc.color_column_index = colorColumn

    # print("svgmap color index : ", svgc.color_column_index)
    # print(svgc.defs)

    svgc.add_content("poi1", "35.000", "134", ["0", "poi1", "tokyo", "東京都新宿区", "f2"])
    print(svgc.contents)
    print(svgc.output_str_to_container())
    # svgc.save_to_container_file("/tmp/sample.svg")

