# redisSvgMap
Dynamic Content Generator and Web Service for Quad Tree Composite Tilied SVGMap using Redis

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

# License
This software is open source software under GPL Ver. 3. Please refer to the LICENSE file.

# Sample data
* The World Cities Database produced by MaxMind is included as sample data for the authoring system. The following is its declaration.
  * This product includes data created by MaxMind, available from http://www.maxmind.com/
