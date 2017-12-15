#!/bin/bash
# chkconfig: 2345 66 36
[ -f /etc/rc.d/init.d/functions ] && . /etc/rc.d/init.d/functions

SERVICE=logkit
PROCESS=logkit
LOGKIT=/usr/local/logkit/package/logkit
PID_FILE="/var/run/logkit.pid"
CONFIG_ARGS="-f /usr/local/logkit/package/logkit.conf > /usr/local/logkit/package/run/logkit.log 2>&1 &"

chmod +x /usr/local/logkit/package/logkit

RETVAL=0

start() {
    echo -n $"Starting logkit daemon: "
    if status $PROCESS &> /dev/null; then
        failure "Already running."
        RETVAL=1
    else
        daemon --pidfile=$PID_FILE --check $SERVICE $LOGKIT $CONFIG_ARGS
        RETVAL=$?
        [ $RETVAL -eq 0 ] && touch /var/lock/subsys/$SERVICE
        echo
        return $RETVAL
    fi
	
    RETVAL=$?
    echo
    return $RETVAL
}

stop() {
    echo -n $"Stopping logkit daemon: "

    killproc -d 10 $PROCESS
    RETVAL=$?
    echo
    [ $RETVAL -eq 0 ] && rm -f /var/lock/subsys/$SERVICE
    return $RETVAL

}

restart() {
   stop
   start
}

case "$1" in
    start|stop|restart)
        $1
        ;;
    status)
        status $PROCESS
        RETVAL=$?
        ;;
    condrestart|try-restart)
        [ -f $LOCKFILE ] && restart || :
        ;;
    reload)
        echo "can't reload configuration, you have to restart it"
        RETVAL=1
        ;;
    *)
        echo $"Usage: $0 {start|stop|status|restart|condrestart|try-restart|reload}"
        exit 1
        ;;
esac
exit $RETVAL
