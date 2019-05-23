#!/bin/sh

prefix=/export/home/radsvr
raddb_dir=$prefix/etc/raddb
lua_root_dir=$prefix/lua_scripts
fake_dir=$prefix/etc/raddb/fakes
bak_dir=$fake_dir/backup


# check fake status
status=`ls -lrt $raddb_dir/mods-enabled/redis|awk -F'r' '{print $1}'`
if [ "$status" = "-" ]; then
    echo "fake is not working, don't stop fake again!"
    exit 0
fi

# turn normal
rm $raddb_dir/mods-enabled/redis && cp $bak_dir/redis $raddb_dir/mods-enabled/redis
rm $lua_root_dir/radius_cfg.lua && cp $bak_dir/radius_cfg.lua $lua_root_dir/radius_cfg.lua


#restart
sudo monit restart radiusd-auth
sudo monit restart radiusd-acct

echo 'fake auth turn off'
