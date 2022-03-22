# 工程简介

# 延伸阅读
kafka消费者命令
kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic ods_base_log

kafka查看主题命令
kafka-topics.sh --list --bootstrap-server hadoop102:9092

kafka消费数据
kafka-console-consumer.sh --topic dwd_display_log --bootstrap-server hadoop102:9092 --from-beginning

行为数据生成
java -jar gmall2020-mock-log-2020-12-18.jar

nginx服务采集日志
logger.sh start

业务数据生成
java -jar gmall2020-mock-db-2020-11-27.jar

业务数据采集
com.yb.gmall.app.ods.FlinkCDC