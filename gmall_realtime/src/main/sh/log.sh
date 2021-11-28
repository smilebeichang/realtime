#!/bin/bash
log_app=gmall-logger-0.0.1-SNAPSHOT.jar
nginx_home=/opt/module/nginx
log_home=/opt/gmall0223

case $1 in
"start")
# 在hadoop162启动nginx
if [[ -z "`ps -ef | awk '/nginx/ && !/awk/{print $n}'`" ]]; then
    echo "在hadoop162启动nginx"
    $nginx_home/sbin/nginx
fi
# 分别在162-164启动日志服务器$
for host in hadoop162 hadoop163 hadoop164 ; do
    echo "在 $host 上启动日志服务器"
    ssh $host "nohup java -jar $log_home/$log_app 1>$log_home/logger.log 2>&1 &"
done
   ;;
"stop")
echo "在hadoop162停止nginx"
$nginx_home/sbin/nginx -s stop
for host in hadoop162 hadoop163 hadoop164 ; do
    echo "在 $host 上停止日志服务器"
    ssh $host "jps | awk '/$log_app/ {print \$1}' | xargs kill -9"
done
   ;;

*)
echo "你启动的姿势不对"
echo " log.sh start 启动日志采集"
echo " log.sh stop  停止日志采集"
   ;;
esac

