import redis
r = redis.Redis(host='192.168.1.145',port=6379,db=0)
r.hset('users:jdoe', 'name', "John Doe")