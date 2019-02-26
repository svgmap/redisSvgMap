# redisSvgMap
Dynamic Content Generator for Quad Tree Composite Tiling SVGMap using Redis

# Dev Env
Python3 and Redis (and pip3 install redis hiredis numpy pillow flask)

# Tests
## Regist test
``python3 csv2redis11.py``

## Delete test
``python3 csv2redis11.py --delete worldcitiespopDelTest_jp.csv``
*Erase all records except gaaja*

## Web Service
### windows
* ``runFlask.bat``
* Access http://localhost:5000/svgmap/

### linux
* ``runFlask.sh``
* Access http://localhost:5000/svgmap/
* nohup ./runFlask.sh >1.txt 2>&1 &
