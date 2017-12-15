# linux 下面将logkit创建为系统服务
将logkit.sh脚本放在/etc/init.d/下面即可，脚本里面定义的logkit可执行文件路径为：/usr/local/logkit/package/logkit，配置文件路径为/usr/local/logkit/package/logkit.conf 日志文件路径为/usr/local/logkit/package/run/logkit.log，可以编辑脚本修改这些参数，脚本需要有可执行权限。  
启动：service logkit start  
停止：service logkit stop  
状态：service logkit status  
重启：service logkit restart

# windows下面将logkit创建为系统服务
运行createLogkitService.bat即可，它依赖nssm.exe，nssm.exe可以在[这里](http://www.nssm.cc/download)下载,将createLogkitService.bat和nssm.exe放在同一个目录下面运行脚本，脚本会自动检测windows系统中有没有logkit服务，没有就创建它并启动，有就检查状态并确保服务已启动。另外还可以在脚本后面跟start和restart参数。  
在windows的系统服务面板中也可以对logkit服务进行管理


