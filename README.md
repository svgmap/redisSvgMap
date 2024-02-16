# redisSvgMap
Dynamic Content Generator and Web Service for [Quad Tree Composite Tilied](https://satakagi.github.io/mapsForWebWS2020-docs/QuadTreeCompositeTilingAndVectorTileStandard.html) SVGMap using Redis

## About
Unlike [SVGMapTools](https://github.com/svgmap/svgMapTools/), this software provides the ability to efficiently perform Quad Tree Composite Tiling on large amounts of point data that are updated successively. Redis, an on-memory database management system, is used for this purpose. And a web service that uses this functionality to provide successively updatable location information has also been implemented.

## Dev Env
Python3 and Redis (and pip3 install redis hiredis numpy pillow flask)

``pip3 install -r requirements.txt``

``pip3 install -r requirements-dev.txt``

## Tests

### Unit test

``python3 -m unittest discover tests``

### display coverage

``coverage run -m unittest discover tests ; coverage report; coverage html``

### export coverage report to html file

``coverage html``

### Regist test
``python3 scripts/csv2redis.py``

### Delete test
``python3 scripts/csv2redis.py --delete worldcitiespopDelTest_jp.csv``
*Erase all records except gaaja*

## Web Service
### windows
* ``runFlaskCustom.bat``
* Access http://localhost:5000/svgmap/

### linux
* ``runFlaskCustom.sh``
* Access http://localhost:5000/svgmap/
* nohup ./runFlask.sh >1.txt 2>&1 &

## Backup and Restore
### Backup
* ``mkdir dump``
* ``python3 dbDump.py``
### Restore
* ``python3 dbDump.py --restore``

## License
This software is open source software under GPL Ver. 3. Please refer to the LICENSE file.

## Sample data
* The World Cities Database produced by MaxMind is included as sample data. The following is its declaration.
  * This product includes data created by MaxMind, available from http://www.maxmind.com/
