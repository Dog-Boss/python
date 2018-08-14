from datetime import datetime
import os
import tarfile

from pathlib import Path

def do_tar(source_dir):
    if not source_dir:
        return
    print("start data file to tar")
    fn = "uploaded_%s.tar.gz" % (datetime.now().strftime("%Y%m%d_%H%M%S"))
    tar_fn = source_dir / fn
    with tarfile.open(str(tar_fn), "w:gz") as tar:
        tar.add(source_dir, arcname=source_dir.name)
    print("finish data file to tar: %s" % tar_fn)
    return tar_fn

do_tar(Path('C:\\Users\\win7\\code'))
try:
	ss = os.stat('C:\\Users\\win7\\code\\ess.py')
	print(ss)
except FileNotFoundError:
	print("yes no the file")
