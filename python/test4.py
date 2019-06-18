
import cx_Oracle

USER_DATA_CONF = {
    'oracle': {
        "dbname": "cq_neva2_product",
        "user": "neva2",
        "password": "neva2_tpgj18"
    }
}

sql = "select p.external_number from  " \
          "t_product p,t_product_extend x " \
          "where p.product_id=x.product_id " \
          "and x.attribute_id=208 " \
          "and x.attribute_value=1"

conf = USER_DATA_CONF['oracle']
try:
    con = cx_Oracle.connect(conf["user"],
                            conf["password"],
                            conf["dbname"])
    cursor = con.cursor()
    cursor.execute(sql)
    
    res = cursor.fetchall()
    user=[]
    for i in res:
        user.append{i[0]}
    print("dmcwuser is {}".format(res))
    print("dmcwuser is {}".format(user))
except Exception:
        pass