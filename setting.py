import redis
REDIS_IP = '127.0.0.1'
REDIS_PORT = '6379'
TASKS = redis.StrictRedis(host=REDIS_IP, port=REDIS_PORT)
