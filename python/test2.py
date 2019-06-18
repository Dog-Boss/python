from datetime import datetime,time,timedelta


now = datetime.now()
print(now)
print(datetime.combine(now, time.min)+timedelta(days=int(1), hours=3))


attributes = ["PRODUCT_SPEC_ID", "ACCOUNT", "NAS_IP", "NAS_PORT", "SESSION_ID"]


solt = -1
def getsolt():
    global solt
    solt = (solt + 1) % 24
    return solt
# for attr in range(1,1):
    # print(getsolt())

a = {
    'name': 'zhanglei',
    'sex': 'man',
}

b = {
    'num': 1413,
    'info': a,
}

a['height'] = 160

print(b)