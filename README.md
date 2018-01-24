# watchman

## 简介
日志采集程序。

## 原理
将一个日志文件以行为单位进行解析。

## 使用方法
1. [下载](https://github.com/maboss-YCMan/watchman.git)源码，并解压 。

2. 将python watchman.py 注册成服务

    配置文件使用 conf/watchman.toml
    
    插件在配置文件里指明
    
## 插件写法
1. 将文件 processor/processor_repo/pluin_prototype.py 复制到其上一层目录
2. 并将名字改成自己想用的名字
3. 在processor 目录下有 processor_path 配置项，设置成自己插件的名字（不含.py）

到此，如果数据库配置好的话，是可以将日志按行上传至数据库中。
接下来的任务，就是在插件的`message_process()`方法里填写自己的处理逻辑。

enjoy！