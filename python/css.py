from datetime import datetime
import os

from pathlib import Path

FILE_NUM = 24
BEFORE_DAY = 2

DATA_DIR = "/home/unitask/data"

REMOTE_HOST = {
    "HOST_00": {
        "host": "222.180.124.129",
        "user": "radscq",
        "path": "/export/home/radscq/backup/auth.{}"
    },
    "HOST_01": {
        "host": "222.180.124.132",
        "user": "radscq",
        "path": "/export/home/radscq/backup/auth.{}"
    },
    "HOST_02": {
        "host": "222.180.124.135",
        "user": "radscq",
        "path": "/export/home/radscq/backup/auth.{}"
    },
    "HOST_03": {
        "host": "222.180.124.138",
        "user": "radscq",
        "path": "/export/home/radscq/backup/auth.{}"
    },
    "HOST_04": {
        "host": "222.180.220.135",
        "user": "radsvr",
        "path": "/home/radsvr/backup/log.{}"
    },
    "HOST_05": {
        "host": "222.180.220.136",
        "user": "radsvr",
        "path": "/home/radsvr/backup/log.{}"
    },
    "HOST_06": {
        "host": "222.180.220.137",
        "user": "radsvr",
        "path": "/home/radsvr/backup/log.{}"
    },
    "HOST_07": {
        "host": "222.180.220.138",
        "user": "radsvr",
        "path": "/home/radsvr/backup/log.{}"
    },
    "HOST_08": {
        "host": "222.180.220.139",
        "user": "radsvr",
        "path": "/home/radsvr/backup/log.{}"
    },
    "HOST_09": {
        "host": "222.180.139.21",
        "user": "radscq",
        "path": "/export/home/radscq/backup/auth.{}"
    },
    "HOST_10": {
        "host": "222.180.139.22",
        "user": "radscq",
        "path": "/export/home/radscq/backup/auth.{}"
    },
    "HOST_11": {
        "host": "222.180.139.26",
        "user": "radscq",
        "path": "/export/home/radscq/backup/auth.{}"
    },
    "HOST_12": {
        "host": "222.180.139.27",
        "user": "radscq",
        "path": "/export/home/radscq/backup/auth.{}"
    }
}


now = datetime.now()
local_base_path = Path(DATA_DIR, "itvauthlog", "itv_{}".format(now.strftime('%Y%m%d')))

for remote_source in REMOTE_HOST.values():
    print(remote_source)
    local_path = Path(local_base_path, remote_source["host"])
    remote_path = Path(remote_source["path"].format(now.strftime("%Y%m")))
    print("finish downloading files from: {}".format(remote_path))
