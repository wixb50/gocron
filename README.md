# gocron - 定时任务管理系统

# 项目简介

> 感谢原项目[ouqiang/gocron](https://github.com/ouqiang/gocron)的支持。 

使用Go语言开发的轻量级定时任务集中调度和管理系统, 用于替代Linux-crontab [查看文档](https://github.com/wixb50/gocron/wiki)

## 功能特性
* Web界面管理定时任务
* crontab时间表达式, 精确到秒
* 任务执行失败可重试
* 任务执行超时, 强制结束
* 任务配置子任务, **DAG工作流支持**
* 账户权限控制
* 任务类型
    * shell任务
    > 在任务节点上执行shell命令, 支持任务同时在多个节点上运行
    * HTTP任务
    > 访问指定的URL地址, 由调度器直接执行, 不依赖任务节点
* 查看任务执行结果日志
* 任务执行结果通知, 支持邮件、Slack、Webhook

### 截图
![流程图](https://raw.githubusercontent.com/wixb50/gocron/master/assets/screenshot/scheduler.png)
![任务](https://raw.githubusercontent.com/wixb50/gocron/master/assets/screenshot/task.png)
![Slack](https://raw.githubusercontent.com/wixb50/gocron/master/assets/screenshot/notification.png)
    
### 支持平台
> Windows、Linux、Mac OS

### 环境要求
>  MySQL、Postgresql、Sqlite3


## 下载
[releases](https://github.com/wixb50/gocron/releases)  

[版本升级](https://github.com/wixb50/gocron/wiki/版本升级)

## 安装

###  二进制安装
1. 解压压缩包   
2. `cd 解压目录`   
3. 启动        
* 调度器启动        
  * Windows: `gocron.exe web`   
  * Linux、Mac OS:  `./gocron web`
* 任务节点启动, 默认监听0.0.0.0:5921
  * Windows:  `gocron-node.exe`
  * Linux、Mac OS:  `./gocron-node`
4. 浏览器访问 http://localhost:5920

### 源码安装

- 安装Go 1.11+
- `go get -d github.com/wixb50/gocron`
- `export GO111MODULE=on` 
- 编译 `make`
- 启动
    * gocron `./bin/gocron web`
    * gocron-node `./bin/gocron-node`


### docker

```shell
docker run --name gocron --link mysql:db -p 5920:5920 -d wixb50/gocron
```

配置: /app/conf/app.ini

日志: /app/log/cron.log

镜像不包含gocron-node, gocron-node需要和具体业务一起构建


### 开发

1. 安装Go1.9+, Node.js v9, Yarn
2. 安装前端依赖 `make install-vue`
3. 启动gocron, gocron-node `make run`
4. 启动node server `make run-vue`, 访问地址 http://localhost:8080

访问http://localhost:8080, API请求会转发给gocron

`make` 编译

`make run` 编译并运行

`make package` 打包 
> 生成当前系统的压缩包 gocron-v1.5-darwin-amd64.tar.gz gocron-node-v1.5-darwin-amd64.tar.gz

`make package-all` 生成Windows、Linux、Mac的压缩包

### 命令

* gocron
    * -v 查看版本

* gocron web
    * --host 默认0.0.0.0
    * -p 端口, 指定端口, 默认5920
    * -e 指定运行环境, dev|test|prod, dev模式下可查看更多日志信息, 默认prod
    * -h 查看帮助
* gocron-node
    * -allow-root *nix平台允许以root用户运行
    * -s ip:port 监听地址  
    * -enable-tls 开启TLS    
    * -ca-file   CA证书文件   
    * -cert-file 证书文件  
    * -key-file  私钥文件
    * -h 查看帮助
    * -v 查看版本

## To Do List
- [x] web节点高可用
- [x] 版本升级
- [x] 批量开启、关闭、删除任务
- [x] 调度器与任务节点通信支持https
- [x] 任务分组
- [x] 多用户
- [x] 权限控制

## 程序使用的组件
* Web框架 [Macaron](http://go-macaron.com/)
* 定时任务调度 [Cron](https://github.com/robfig/cron)
* ORM [Xorm](https://github.com/go-xorm/xorm)
* UI框架 [Element UI](https://github.com/ElemeFE/element)
* 依赖管理 [Govendor](https://github.com/kardianos/govendor)
* RPC框架 [gRPC](https://github.com/grpc/grpc)

## 反馈
提交[issue](https://github.com/wixb50/gocron/issues/new)

## ChangeLog

[v1.5.5]()
--------
* 支持web多点主从高可用

[v1.5.4]()
--------
* 支持Sqlite3数据库
* 任务依赖支持DAG工作流
    * 需要有效dag才会执行，否则只执行根任务
    * dag的任意子任务更改，需手动开关根任务使其生效
* 支持shell任务调度策略
* 支持最近一次失败任务继续运行

v1.5
--------
* 前端使用Vue+ElementUI重构
* 任务通知
    * 新增WebHook通知
    * 自定义通知模板
    * 匹配任务执行结果关键字发送通知
* 任务列表页显示任务下次执行时间

v1.4
--------
* HTTP任务支持POST请求
* 后台手动停止运行中的shell任务
* 任务执行失败重试间隔时间支持用户自定义
* 修复API接口调用报403错误

v1.3
--------
* 支持多用户登录
* 增加用户权限控制


v1.2.2
--------
* 用户登录页增加图形验证码
* 支持从旧版本升级
* 任务批量开启、关闭、删除
* 调度器与任务节点支持HTTPS双向认证
* 修复任务列表页总记录数显示错误

v1.1
--------

* 任务可同时在多个节点上运行
* *nix平台默认禁止以root用户运行任务节点
* 子任务命令中增加预定义占位符, 子任务可根据主任务运行结果执行相应操作
* 删除守护进程模块
* Web访问日志输出到终端
