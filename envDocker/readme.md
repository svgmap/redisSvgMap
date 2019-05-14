* docker build . -t python:redisSvgMap

* docker-compose
** docker-compose
** docker exec -ti ..._comp_python37_1 ash

* plane docker
** docker container run --name some-redis -d -p 6379:6379 redis
** docker run --name python37 -v pythonShared:/data --link some-redis:localhost -p 8080:5000 -i -t python:redisSvgMap /bin/ash 
