import pymysql

DATABASES = {
    'sessiondb': {
        'NAME': 'sessiondb',
        'USER': 'neva2',
        'PASSWORD': 'neva2_tpgj18',
        'HOST': 'vsessiondb-1',
        'PORT': 3306,
    },
}


def check_session(account, product_spec_id):
    conn = pymysql.connect(
        host=DATABASES['sessiondb']['HOST'],
        port=DATABASES['sessiondb']['PORT'],
        user=DATABASES['sessiondb']['USER'],
        password=DATABASES['sessiondb']['PASSWORD'],
        db=DATABASES['sessiondb']['NAME'],
        charset='utf8')

    try:
        with conn.cursor() as cursor:
            sql = "select `SESSION_ID`,`NAS_IP` from t_user_sessions_info " \
                  "where `account`=%s and `product_spec_id`=%s"
            cursor.execute(sql, (account, product_spec_id))
            return cursor.fetchall()
    finally:
        conn.close()

if __name__ == "__main__":
    account = '02367665929'
    product_spec_id = 1
    sessioninfo_list = check_session(account, product_spec_id)
    for row in sessioninfo_list:
        print(row[0], row[1])