#!/bin/bash
flink=/opt/module/flink-yarn/bin/flink
jar=/opt/module/mock/app_log/spring-boot/gmall_realtime-1.0-SNAPSHOT.jar

apps=(
    cn.edu.sysu.app.DWDLogApp
)

# 遍历数组
for app in ${apps[*]} ; do
    $flink run -d -c $app $jar
done


