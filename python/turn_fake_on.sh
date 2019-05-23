#!/bin/sh

prefix=/export/home/radsvr
raddb_dir=$prefix/etc/raddb
lua_root_dir=$prefix/lua_scripts
fake_dir=$prefix/etc/raddb/fakes
bak_dir=$fake_dir/backup

# check fake status
status=`ls -lrt $raddb_dir/mods-enabled/redis|awk -F'r' '{print $1}'`
if [ "$status" = "l" ]; then
    echo "fake is working,don't start fake again!"
    exit 0
fi

# backup
cp -rp $raddb_dir/mods-enabled/redis $bak_dir && rm $raddb_dir/mods-enabled/redis
cp -rp $lua_root_dir/radius_cfg.lua $bak_dir  && rm $lua_root_dir/radius_cfg.lua


# turn fake
ln -s $fake_dir/redis $raddb_dir/mods-enabled/redis 
ln -s $fake_dir/radius_cfg.lua $lua_root_dir/radius_cfg.lua  


#restart
sudo monit restart radiusd-auth
sudo monit restart radiusd-acct

echo 'fake auth turn on'