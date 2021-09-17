// Command gocron
//go:generate statik -src=../../web/public -dest=../../internal -f

package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	macaron "gopkg.in/macaron.v1"

	"github.com/ouqiang/gocron/internal/models"
	"github.com/ouqiang/gocron/internal/modules/app"
	"github.com/ouqiang/gocron/internal/modules/logger"
	"github.com/ouqiang/gocron/internal/modules/setting"
	"github.com/ouqiang/gocron/internal/routers"
	"github.com/ouqiang/gocron/internal/service"
	"github.com/ouqiang/goutil"
	"github.com/urfave/cli"
)

var (
	AppVersion           = "1.5"
	BuildDate, GitCommit string
)

// web服务器默认端口
const DefaultPort = 5920

func main() {
	cliApp := cli.NewApp()
	cliApp.Name = "gocron"
	cliApp.Usage = "gocron service"
	cliApp.Version, _ = goutil.FormatAppVersion(AppVersion, GitCommit, BuildDate)
	cliApp.Commands = getCommands()
	cliApp.Flags = append(cliApp.Flags, []cli.Flag{}...)
	err := cliApp.Run(os.Args)
	if err != nil {
		logger.Fatal(err)
	}
}

// getCommands
func getCommands() []cli.Command {
	command := cli.Command{
		Name:   "web",
		Usage:  "run web server",
		Action: runWeb,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "host",
				Value: "0.0.0.0",
				Usage: "bind host",
			},
			cli.IntFlag{
				Name:  "port,p",
				Value: DefaultPort,
				Usage: "bind port",
			},
			cli.StringFlag{
				Name:  "address",
				Value: fmt.Sprintf("127.0.0.1:%d", DefaultPort),
				Usage: "bind address",
			},
			cli.StringFlag{
				Name:  "env,e",
				Value: "prod",
				Usage: "runtime environment, dev|test|prod",
			},
		},
	}

	return []cli.Command{command}
}

func runWeb(ctx *cli.Context) {
	// 设置运行环境
	setEnvironment(ctx)
	// 初始化应用
	app.InitEnv(AppVersion)
	// 初始化http服务
	host := parseHost(ctx)
	port := parsePort(ctx)
	maddress := ctx.String("address")
	srv := &http.Server{
		Addr: fmt.Sprintf("%s:%d", host, port),
	}
	// 选择运行的角色
	master := models.Setting{}
	master.NewMaster(maddress)
	initModule(false)
	// 主节点5s一次心跳，从节点20s一次
	retry := 0
	for {
		time.Sleep(time.Duration(retry) * time.Second)
		if !app.Installed {
			if retry == 0 {
				srv = runMaster(ctx, srv)
				retry = 5
				logger.Infof("主节点#请先安装#-%s", master.Key)
			}
			continue
		}
		if ok, address := master.TryMaster(); ok {
			if retry != 5 {
				srv = runMaster(ctx, srv)
				retry = 5
				logger.Infof("主节点#选举成功#-%s", address)
			}
			logger.Infof("主节点#发送心跳#-%s-%s", master.Key, master.Value)
		} else {
			if retry != 20 || maddress != address {
				srv = runProxy(ctx, srv, address)
				maddress = address
				retry = 20
				logger.Infof("从节点#切换成功#-%s", address)
			}
			logger.Infof("从节点#请求转发#-%s-%s", master.Key, address)
		}
	}
}

func runMaster(ctx *cli.Context, lsrv *http.Server) *http.Server {
	lsrv.Shutdown(context.Background())
	service.ServiceTask.WaitAndExit()
	// 初始化模块 DB、定时任务等
	initModule(true)
	// 捕捉信号,配置热更新等
	go catchSignal()
	m := macaron.Classic()
	// 注册路由
	routers.Register(m)
	// 注册中间件.
	routers.RegisterMiddleware(m)
	mux := http.NewServeMux()
	mux.Handle("/", m)
	srv := &http.Server{
		Addr:    lsrv.Addr,
		Handler: mux,
	}
	go srv.ListenAndServe()
	return srv
}

func runProxy(ctx *cli.Context, lsrv *http.Server, address string) *http.Server {
	lsrv.Shutdown(context.Background())
	service.ServiceTask.WaitAndExit()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		target, _ := url.Parse(fmt.Sprintf("http://%s", address))
		proxy := httputil.NewSingleHostReverseProxy(target)
		proxy.ServeHTTP(w, req)
	})
	srv := &http.Server{
		Addr:    lsrv.Addr,
		Handler: mux,
	}
	go srv.ListenAndServe()
	return srv
}

func initModule(withTask bool) {
	if !app.Installed {
		return
	}

	if app.Setting == nil {
		config, err := setting.Read(app.AppConfig)
		if err != nil {
			logger.Fatal("读取应用配置失败", err)
		}
		app.Setting = config

		// 初始化DB
		models.Db = models.CreateDb()

		// 版本升级
		upgradeIfNeed()
	}

	if withTask {
		// 初始化定时任务
		service.ServiceTask.Initialize()
	}
}

// 解析端口
func parsePort(ctx *cli.Context) int {
	port := DefaultPort
	if ctx.IsSet("port") {
		port = ctx.Int("port")
	}
	if port <= 0 || port >= 65535 {
		port = DefaultPort
	}

	return port
}

func parseHost(ctx *cli.Context) string {
	if ctx.IsSet("host") {
		return ctx.String("host")
	}

	return "0.0.0.0"
}

func setEnvironment(ctx *cli.Context) {
	env := "prod"
	if ctx.IsSet("env") {
		env = ctx.String("env")
	}

	switch env {
	case "test":
		macaron.Env = macaron.TEST
	case "dev":
		macaron.Env = macaron.DEV
	default:
		macaron.Env = macaron.PROD
	}
}

// 捕捉信号
func catchSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	for {
		s := <-c
		logger.Info("收到信号 -- ", s)
		switch s {
		case syscall.SIGHUP:
			logger.Info("收到终端断开信号, 忽略")
		case syscall.SIGINT, syscall.SIGTERM:
			shutdown()
		}
	}
}

// 应用退出
func shutdown() {
	defer func() {
		logger.Info("已退出")
		os.Exit(0)
	}()

	if !app.Installed {
		return
	}
	logger.Info("应用准备退出")
	// 停止所有任务调度
	logger.Info("停止定时任务调度")
	service.ServiceTask.WaitAndExit()
}

// 判断应用是否需要升级, 当存在版本号文件且版本小于app.VersionId时升级
func upgradeIfNeed() {
	currentVersionId := app.GetCurrentVersionId()
	// 没有版本号文件
	if currentVersionId == 0 {
		return
	}
	if currentVersionId >= app.VersionId {
		return
	}

	migration := new(models.Migration)
	logger.Infof("版本升级开始, 当前版本号%d", currentVersionId)

	migration.Upgrade(currentVersionId)
	app.UpdateVersionFile()

	logger.Infof("已升级到最新版本%d", app.VersionId)
}
