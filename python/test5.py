import redis
from redis import Redis, ConnectionPool
import _thread

REDIS_CFG = {
    "host": "192.168.44.121",
    "port": 19000,
    "password": 'Neva3@)!^'
}

def getsolt(threadName):
    solt_key = "dacs:timer:solt"
    redis_cli=redis.Redis("192.168.44.121", 19000, password='Neva3@)!^')

    slot = redis_cli.incr(solt_key)
    print("{}  {}".format(threadName,slot%24))


if __name__ == "__main__":
    for i in range(0,30):
        try:
            _thread.start_new_thread(getsolt, ("Thread-{}".format(i)))
        except Exception:

            print()
