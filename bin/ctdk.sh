#!/bin/bash
##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2012-12-28 15:32:35 macan>
#
# This is the mangement script for Pomegranate
#
# Armed with EMACS.

SYSCTL_ADJ_SYN="sysctl -w net.ipv4.tcp_max_syn_backlog=8192;"

if [ "x$HVFS_HOME" == "x" ]; then
    HVFS_HOME=`pwd`
    TAIL=`basename $HVFS_HOME`
    if [ "x$TAIL" == 'xbin' ]; then
        HVFS_HOME=`dirname $HVFS_HOME`
    fi
fi

if [ "x$CFILE" == "x" ]; then
    CONFIG_FILE="$HVFS_HOME/conf/ctdk.conf"
else
    CONFIG_FILE="$HVFS_HOME/conf/$CFILE"
fi

if [ "x$LOG_DIR" == "x" ]; then
    LOG_DIR="$HVFS_HOME/logs"
    if [ ! -d $LOG_DIR ]; then
        mkdir $LOG_DIR
    fi
fi
export LOG_DIR

if [ "x$STORE_DIR" == "x" ]; then
    STORE_DIR="$HVFS_HOME/store"
    if [ ! -d $STORE_DIR ]; then
        mkdir $STORE_DIR
    fi
fi
export STORE_DIR

if [ "x$PASSWD" == "x" ]; then
    # it is the normal mode, we do not use expect
    SSH="ssh -x"
    SCP="scp"
else
    # we have to use expect to login
    SSH="$HVFS_HOME/bin/rexec.exp $PASSWD"
    if [ ! -x /usr/bin/expect ]; then
        echo "/usr/bin/expect does not exist. Use 'which expect' to find it and update the header in file 'rexec.exp'."
    fi
fi

if [ "x$USERNAME" == "x" ]; then
    UN=""
else
    UN="$USERNAME@"
fi

function do_conf_check() {
    if [ -d $HVFS_HOME/conf ]; then
        if [ -e $1 ]; then
        # It is ok to continue
            return
        else
            echo "Missing config files: $1."
            echo "Please check your home path, and make sure the config file"
            echo "'hvfs.conf' is in HVFS_HOME/conf/."
            exit
        fi
    else
        echo "Corrupt home path: $HVFS_HOME."
        echo "Please check your home path, and make sure the config file"
        echo "'hvfs.conf' is in HVFS_HOME/conf/."
        exit
    fi
}

function isdigit ()    # Tests whether *entire string* is numerical.
{             # In other words, tests for integer variable.
  [ $# -eq 1 ] || return 1

  case $1 in
    *[!0-9]*|"") return 1;;
              *) return 0;;
  esac
}

# check if the config file exists.
do_conf_check $CONFIG_FILE
do_conf_check $HVFS_HOME/conf/redis.conf

# Read the config file and start the servers 

# Construct the client command line
if [ -e $HVFS_HOME/conf/client.conf ]; then
    # Using the config file
    ARGS=`cat $HVFS_HOME/conf/client.conf | grep -v "^ *#" | grep -v "^$"`
    RC_CMD="LD_LIBRARY_PATH=$HVFS_HOME/lib config=$CONFIG_FILE "`echo $ARGS`
fi

# Construct the server command line
if [ -e $HVFS_HOME/conf/server.conf ]; then
    # Using the config file
    ARGS=`cat $HVFS_HOME/conf/server.conf | grep -v "^ *#" | grep -v "^$" | grep -v "fsid="`
    RS_CMD="LOG_DIR=$LOG_DIR "`echo $ARGS`
fi

# Construct the server config file
function gen_config() {
    if [ "x$1" == "x" ]; then
        id=0
        port=6739
    elif [ "x$2" == "x" ]; then
        id=$1
        port=6739
    else
        id=$1
        port=$2
    fi
    IP=$3

    if [ -e $HVFS_HOME/conf/redis.conf ]; then
        cat $HVFS_HOME/conf/redis.conf | \
            sed -e 's|^pidfile \(.*\)|pidfile '$LOG_DIR'/server.'$id'.pid|' \
            -e 's|^port \(.*\)|port '$port'|' \
            -e 's|^dbfilename \(.*\)|dbfilename '$STORE_DIR'/server.'$id'.rdb|' \
            > $HVFS_HOME/conf/redis.$id.$port.conf
        $SCP $HVFS_HOME/conf/redis.$id.$port.conf $UN$IP:$HVFS_HOME/conf/
    fi
}

function adjust_syn() {
    ipnr=`cat $CONFIG_FILE | grep "^redis:" | awk -F: '{print $2":"$4":"$3}'`
    for x in $ipnr; do
        ip=`echo $x | awk -F: '{print $1}'`
        $SSH $UN$ip "$SYSCTL_ADJ_SYN" > /dev/null &
    done
    echo "Adjust SYN on redis server done."
}

function start_client() {
    if ! isdigit $1; then
        ipnr=`cat $CONFIG_FILE | grep "^client:" | awk -F: '{print $2":"$4":"$3}'`
        for x in $ipnr; do 
            ip=`echo $x | awk -F: '{print $1}'`
            id=`echo $x | awk -F: '{print $2}'`
            port=`echo $x | awk -F: '{print $3}'`
            RC_CMD=`echo $RC_CMD | sed -e 's|\(.*\)$HOME\(.*\)|\1'$HVFS_HOME'\2|'`
            $SSH $UN$ip "cd $HVFS_HOME; $RC_CMD $HVFS_HOME/ctdk_huadan_1d -d $id $@ > $LOG_DIR/client.$id.log" > /dev/null &
        done
        echo "Start clients done."
    else
        ipnr=`cat $CONFIG_FILE | grep "^client:.*:$1\$" | awk -F: '{print $2":"$4":"$3}'`
        for x in $ipnr; do 
            ip=`echo $x | awk -F: '{print $1}'`
            id=`echo $x | awk -F: '{print $2}'`
            port=`echo $x | awk -F: '{print $3}'`
            RC_CMD=`echo $RC_CMD | sed -e 's|\(.*\)$HOME\(.*\)|\1'$HVFS_HOME'\2|'`
            $SSH $UN$ip "cd $HVFS_HOME; $RC_CMD $HVFS_HOME/ctdk_huadan_1d -d $id $@ > $LOG_DIR/client.$id.log" > /dev/null &
            echo "Start client $id done."
        done
    fi
}

function start_server() {
    if [ "x$1" == "x" ]; then
        ipnr=`cat $CONFIG_FILE | grep "^redis:" | awk -F: '{print $2":"$4":"$3}'`
        for x in $ipnr; do 
            ip=`echo $x | awk -F: '{print $1}'`
            id=`echo $x | awk -F: '{print $2}'`
            port=`echo $x | awk -F: '{print $3}'`
            gen_config $id $port $ip
            $SSH $UN$ip "$RS_CMD $HVFS_HOME/bin/redis-server $HVFS_HOME/conf/redis.$id.$port.conf > $LOG_DIR/server.$id.log" > /dev/null &
            #unlink $HVFS_HOME/conf/redis.$id.$port.conf
        done
        echo "Start Redis server done."
    else
        ipnr=`cat $CONFIG_FILE | grep "^redis:.*:$1\$" | awk -F: '{print $2":"$4":"$3}'`
        for x in $ipnr; do 
            ip=`echo $x | awk -F: '{print $1}'`
            id=`echo $x | awk -F: '{print $2}'`
            port=`echo $x | awk -F: '{print $3}'`
            gen_config $id $port
            $SSH $UN$ip "$RS_CMD $HVFS_HOME/bin/redis-server $HVFS_HOME/conf/redis.$id.$port.conf > $LOG_DIR/server.$id.log" > /dev/null &
            #unlink $HVFS_HOME/conf/redis.$id.$port.conf
            echo "Start Redis server $id done."
        done
    fi
}

function start_watcher() {
    WATCH_DIR="/mnt/data1/src-data/tt/tt_ori_log/"
    WATCH_INTV=10
    if [ "x$1" == "x" ]; then
        ENDNR=1000
    else
        ENDNR=$1
    fi

    THISNR=0
    ipnr=`cat $CONFIG_FILE | grep "^redis:" | awk -F: '{print $2":"$4":"$3}'`
    for x in $ipnr; do 
        if [ $THISNR -ge $ENDNR ]; then
            break;
        fi
        ip=`echo $x | awk -F: '{print $1}'`
        port=`echo $x | awk -F: '{print $3}'`
        id=`echo $x | awk -F: '{print $2}'`
        if [ "x$id" == "x0" ]; then
            WIP=$ip
            WPORT=$port
        fi
        $SSH $UN$ip "cd $HVFS_HOME/; LD_LIBRARY_PATH=$HVFS_HOME/lib ./watcher -r $WIP -p $WPORT -d $WATCH_DIR -i $WATCH_INTV -I $id > $LOG_DIR/watcher.$id.log" > /dev/null &
        let THISNR=$THISNR+1
    done
    echo "Start Redis server done."
}

function stop_client() {
    if [ "x$1" == "x" ]; then
        ipnr=`cat $CONFIG_FILE | grep "^client:" | awk -F: '{print $2":"$4}'`
    else
        ipnr=`cat $CONFIG_FILE | grep "^client:.*:$1\$" | awk -F: '{print $2":"$4}'`
    fi

    for x in $ipnr; do 
        ip=`echo $x | awk -F: '{print $1}'`
        id=`echo $x | awk -F: '{print $2}'`
        PID=`$SSH $UN$ip "ps aux" | grep "ctdk_huadan_1d -d $id" | grep -v bash | grep -v ssh | grep -v expect | grep -v grep | awk '{print $2}'`
        $SSH $UN$ip "kill -s SIGTERM $PID 2>&1 > /dev/null" > /dev/null
        echo "stop client[$id] pid $PID."
    done
}

function stop_server() {
    if [ "x$1" == "x" ]; then
        ipnr=`cat $CONFIG_FILE | grep "^redis:" | awk -F: '{print $2":"$4}'`
    else
        ipnr=`cat $CONFIG_FILE | grep "^redis:.*:$1\$" | awk -F: '{print $2":"$4}'`
    fi

    for x in $ipnr; do 
        ip=`echo $x | awk -F: '{print $1}'`
        id=`echo $x | awk -F: '{print $2}'`
        PID=`$SSH $UN$ip "cat $LOG_DIR/server.$id.pid"`
        $SSH $UN$ip "kill -s SIGTERM $PID 2>&1 > /dev/null" > /dev/null
        echo "stop server[$id] pid $PID."
    done
}

function kill_client() {
    if [ "x$1" == "x" ]; then
        ipnr=`cat $CONFIG_FILE | grep "^client:" | awk -F: '{print $2":"$4}'`
    else
        ipnr=`cat $CONFIG_FILE | grep "^client:.*:$1\$" | awk -F: '{print $2":"$4}'`
    fi

    for x in $ipnr; do 
        ip=`echo $x | awk -F: '{print $1}'`
        id=`echo $x | awk -F: '{print $2}'`
        PID=`$SSH $UN$ip "ps aux" | grep "ctdk_huadan_1d -d $id" | grep -v bash | grep -v ssh | grep -v expect | grep -v grep | awk '{print $2}'`
        $SSH $UN$ip "kill -9 $PID 2>&1 > /dev/null" > /dev/null
        echo "kill client[$id] pid $PID."
    done
}

function kill_server() {
    if [ "x$1" == "x" ]; then
        ipnr=`cat $CONFIG_FILE | grep "^redis:" | awk -F: '{print $2":"$4}'`
    else
        ipnr=`cat $CONFIG_FILE | grep "^redis:.*:$1\$" | awk -F: '{print $2":"$4}'`
    fi

    for x in $ipnr; do 
        ip=`echo $x | awk -F: '{print $1}'`
        id=`echo $x | awk -F: '{print $2}'`
        PID=`$SSH $UN$ip "cat $LOG_DIR/server.$id.pid"`
        $SSH $UN$ip "kill -9 $PID 2>&1 > /dev/null" > /dev/null
        echo "kill server[$id] pid $PID."
    done
}

function start_all() {
    start_server
    #start_client
}

function stop_all() {
    stop_client
    stop_server
}

function kill_all() {
    kill_client
    kill_server
}

function stat_client() {
    echo "----------CLIENT-----------"
    ipnr=`cat $CONFIG_FILE | grep "^client:" | awk -F: '{print $2":"$4}'`
    for x in $ipnr; do 
        ip=`echo $x | awk -F: '{print $1}'`
        id=`echo $x | awk -F: '{print $2}'`
        #NR=`$SSH $UN$ip "ps aux" | grep "ctdk_huadan_1d -d $id" | grep -v bash | grep -v ssh | grep -v expect | grep -v grep | wc -l`
        CPID=`$SSH $UN$ip "ps aux" | grep "ctdk_huadan_1d -d $id" | grep -v bash | grep -v ssh | grep -v expect | grep -v grep | awk '{print $2}'`
        if [ "x$CPID" == "x" ]; then
            echo "CLIENT $id is gone."
        else
            for f in $CPID; do
                if [ "$f" -gt "1" ]; then
                    echo "CLIENT $id is running (ip: $ip, pid: $f)."
                fi
            done
        fi
    done
}

function stat_server() {
    echo "----------SERVER-----------"
    ipnr=`cat $CONFIG_FILE | grep "^redis:" | awk -F: '{print $2":"$4}'`
    for x in $ipnr; do 
        ip=`echo $x | awk -F: '{print $1}'`
        id=`echo $x | awk -F: '{print $2}'`
        NR=`$SSH $UN$ip "if [ -e $LOG_DIR/server.$id.pid ]; then cat $LOG_DIR/server.$id.pid; else echo -1; fi"`
        if [ "x$NR" == "x-1" ]; then
            echo "SERVER $id is gone."
        else
            echo "SERVER $id is running (ip: $ip, pid: $NR)."
        fi
    done
}

function do_status() {
    echo "Checking clusters' status ..."
    stat_server
    stat_client
}

function do_size() {
    echo "Calculate DBSize now ..."
    ipnr=`cat $CONFIG_FILE | grep "^redis:" | awk -F: '{print $2":"$4":"$3}'`
    TNR=0
    for x in $ipnr; do 
        ip=`echo $x | awk -F: '{print $1}'`
        id=`echo $x | awk -F: '{print $2}'`
        port=`echo $x | awk -F: '{print $3}'`
        echo "Server $id {"
        NR=`echo INFO | $HVFS_HOME/bin/redis-cli -h $ip -p $port | grep "^db[0-9]*:" | sed -e "s/^\(.*\)/->\t\1/g"`
        echo -e "$NR"
        echo "}"
    done
}

function do_cmd() {
    echo "Do commands now ..."
    ipnr=`cat $CONFIG_FILE | grep "^redis:" | awk -F: '{print $2":"$4":"$3}'`
    TNR=0
    for x in $ipnr; do 
        ip=`echo $x | awk -F: '{print $1}'`
        id=`echo $x | awk -F: '{print $2}'`
        port=`echo $x | awk -F: '{print $3}'`
        echo "Server $id {"
        echo $@ | $HVFS_HOME/bin/redis-cli -h $ip -p $port | cut -f2
        echo "}"
    done
    echo "Done!"
}

function do_clean() {
    echo "Flush Database Now ..."
    ipnr=`cat $CONFIG_FILE | grep "^redis:" | awk -F: '{print $2":"$4":"$3}'`
    TNR=0
    for x in $ipnr; do 
        ip=`echo $x | awk -F: '{print $1}'`
        id=`echo $x | awk -F: '{print $2}'`
        port=`echo $x | awk -F: '{print $3}'`
        echo -n "Server $id "
        echo FLUSHALL | $HVFS_HOME/bin/redis-cli -h $ip -p $port | cut -f2
    done
    echo "All Clear!"
}

function do_help() {
    echo "Version 0.0.1b"
    echo "Copyright (c) 2012 Can Ma <ml.macana@gmail.com>"
    echo ""
    echo "Usage: ctdk.sh [start|stop|kill] [server|client] [id] [ARGS]"
    echo "               [clean|stat]"
    echo ""
    echo "Commands:"
    echo "      start [t] [id]  start servers"
    echo "      stop [t] [id]   stop servers"
    echo "      kill [t] [id]   kill servers"
    echo "      clean           clean the STOREGE home"
    echo "      stat            get and print servers' status"
    echo ""
    echo "Environments:"
    echo "      HVFS_HOME       default to the current path."
    echo "                      Note that, if you boot servers on other nodes, "
    echo "                      you have to ensure that all the binaries are "
    echo "                      in the right pathname (same as this node)."
    echo "      LOG_DIR         default to ~"
    echo ""
    echo "      USERNAME        default user name for each ssh connection."
    echo "      PASSWD          default passwd for each ssh connection."
    echo ""
    echo "Examples:"
    echo "1. get the current status"
    echo "   $ ctdk.sh stat"
}

if [ "x$1" == "xstart" ]; then
    if [ "x$2" == "xserver" ]; then
        start_server $3
    elif [ "x$2" == "xclient" ]; then
        shift 2
        start_client $@
    else
        start_all
    fi
elif [ "x$1" == "xwatcher" ]; then
    shift 1
    start_watcher $@
elif [ "x$1" == "xstop" ]; then
    if [ "x$2" == "xserver" ]; then
        stop_server $3
    elif [ "x$2" == "xclient" ]; then
        stop_client $3
    else
        stop_all
    fi
elif [ "x$1" == "xkill" ]; then
    if [ "x$2" == "xserver" ]; then
        kill_server
    elif [ "x$2" == "xclient" ]; then
        kill_client
    else
        kill_all
    fi
elif [ "x$1" == "xstat" ]; then
    if [ "x$2" == "x" ]; then
        do_status
    fi
elif [ "x$1" == "xsize" ]; then
    if [ "x$2" == "x" ]; then
        do_size
    fi
elif [ "x$1" == "xcmd" ]; then
    shift 1
    do_cmd $@
elif [ "x$1" == "xclean" ]; then
    do_clean
elif [ "x$1" == "xrestart" ]; then
    stop_all
    start_all
elif [ "x$1" == "xsyn" ]; then
    adjust_syn
elif [ "x$1" == "xhelp" ]; then
    do_help
else
    do_help
fi
