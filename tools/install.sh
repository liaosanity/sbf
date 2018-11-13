#/bin/sh

if [ $# -ne 1 ]
then
    echo "Usage: sh `basename $0` debug|demo"
    exit -1
fi

arg=$1
host=""
log=""

if [ $arg = "debug" ]
then
    host="debug"
    log="local"
elif [ $arg = "demo" ]
then
    host="demo"
    log="demo"
else
    echo "Usage: sh `basename $0` debug|demo"
    exit -1
fi

# etc
ln -sf /home/test/sbf/etc/sbf_${host}.conf /home/test/sbf/etc/sbf.conf

# logging
ln -sf /home/test/sbf/etc/logging.conf.$log /home/test/sbf/etc/logging.conf

# lib
/sbin/ldconfig -n /home/test/sbf/lib > /dev/null 2>&1

# module
/sbin/ldconfig -n /home/test/sbf/module > /dev/null 2>&1
ln -sf /home/test/sbf/module/libsbf.so.1 /home/test/sbf/module/libsbf.so

