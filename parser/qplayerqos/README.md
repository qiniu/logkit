## kafka内播放器qos打点数据解析

播放器Qos数据经过pili-qos服务收集，pili-flume服务转发，发送到kafka的多个不同队列中。发送到kafka后，logkit可以支持从kafka消费数据，发送日志到pandora。Qos数据的格式为每条一行，表示一个事件，每个事件的各个字段由制表符（tab,\t）分割，例如（注意：应将以下的所有连续空格替换为tab）：
```
60.220.72.155   play_end.v5     1605518858498   400326AB-BBC7-4A31-B7D2-BD64135AE75C    1.1.0.79        https   staticsg.bldimg.com     videos/ticktocks/21614546/21614546_1546598855.mp4       -       112.83.191.125  1605518794889   1605518858498   0       0       1074825 0       999     iPhone7,2       iOS     12.4    player.zhuling.com      3.4.3   0.00    0.16    0.96    0.05    ffmpeg-3.0      -       WIFI    100.100.69.95   112.83.191.125  Qiniu-Mobile    -       0       0       0       0       0       -       -       -       player.zhuling.com      0       session_1
```

目前kafka中数据是 avro binary 格式，logkit暂时没有对该格式进行支持，因此这里自己实现了一个parser，解析avro binary格式以及日志中各字段的含义，发送到pandora.