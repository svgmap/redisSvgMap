# redisSvgMap
Dynamic Content Generator for Quad Tree Composite Tiling SVGMap using Redis

# Dev Env
Python3 and Redis (and pip install redis , pip install hiredis)

# Tests
## Regist test
``python csv2redis11.py``

## Delete test
``python csv2redis11.py --delete worldcitiespopDelTest_jp.csv``
*Erase all records except gaaja*

## Web Service
``runFlask.bat``
Access http://localhost:5000/svgmap/
