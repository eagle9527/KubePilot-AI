// main.go - KubePilot AI Controller 入口
// KubePilot AI 是一个运行在 Kubernetes 内部的 AI SRE Controller
// 通过监听集群事件，调用大模型分析，生成运维结论，并推送通知
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	kubeaiiov1 "github.com/kubepilot-ai/kubeai-controller/api/v1"
	"github.com/kubepilot-ai/kubeai-controller/controllers"
	"github.com/kubepilot-ai/kubeai-controller/pkg/config"
	"github.com/kubepilot-ai/kubeai-controller/pkg/inspection"
	"github.com/kubepilot-ai/kubeai-controller/pkg/llm"
	"github.com/kubepilot-ai/kubeai-controller/pkg/notifier"
)

var (
	// Scheme 构建 Scheme
	Scheme = runtime.NewScheme()
	// SetupLog 是初始化日志
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	// 注册内置 Scheme
	utilruntime.Must(clientgoscheme.AddToScheme(Scheme))
	// 注册 KubePilot AI Scheme
	utilruntime.Must(kubeaiiov1.AddToScheme(Scheme))
}

func main() {
	var (
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
		leaderElectionID     string
	)

	// 解析命令行参数
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&leaderElectionID, "leader-election-id", "kubepilot-ai-controller",
		"The ID for leader election.")
	flag.Parse()

	// 配置日志
	ctrl.SetLogger(zap.New(zap.UseDevMode(true), zap.StacktraceLevel(zapcore.ErrorLevel)))

	setupLog.Info("Starting KubePilot AI Controller",
		"version", "v0.1.0",
		"metrics-addr", metricsAddr,
		"probe-addr", probeAddr,
	)

	// 加载配置
	cfg, err := config.LoadConfig()
	if err != nil {
		setupLog.Error(err, "Failed to load configuration")
		os.Exit(1)
	}

	restCfg := ctrl.GetConfigOrDie()

	// 创建 Manager
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                 Scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       leaderElectionID,
	})
	if err != nil {
		setupLog.Error(err, "Unable to start manager")
		os.Exit(1)
	}

	// 初始化 LLM Manager
	llmManager := llm.NewManager()
	if err := llmManager.InitializeFromConfig(cfg.LLM.Providers, cfg.LLM.DefaultProvider); err != nil {
		setupLog.Error(err, "Failed to initialize LLM manager")
		os.Exit(1)
	}

	// 初始化通知管理器
	notifierManager := notifier.NewManager(&notifier.Filters{
		MinLevel:          notifier.MessageLevel(cfg.Notification.Filter.MinLevel),
		IncludeNamespaces: cfg.Notification.Filter.IncludeNamespaces,
		ExcludeNamespaces: cfg.Notification.Filter.ExcludeNamespaces,
		IncludeResources:  cfg.Notification.Filter.IncludeResources,
		ExcludeResources:  cfg.Notification.Filter.ExcludeResources,
		CooldownPeriod:    cfg.Notification.CooldownPeriod,
		MaxPerMinute:      20,
	})

	// 注册企业微信通知器
	if cfg.Notification.WeChat.Enabled && cfg.Notification.WeChat.WebhookURL != "" {
		wechatNotifier := notifier.NewWeChatNotifier(cfg.Notification.WeChat.WebhookURL)
		notifierManager.Register("wechat", wechatNotifier)
		setupLog.Info("Registered WeChat notifier")
	}

	// 注册钉钉通知器
	if cfg.Notification.DingTalk.Enabled {
		dingtalkNotifier := notifier.NewDingTalkNotifier(
			cfg.Notification.DingTalk.WebhookURL,
			cfg.Notification.DingTalk.AccessToken,
			cfg.Notification.DingTalk.Secret,
			cfg.Notification.DingTalk.AtMobiles,
			cfg.Notification.DingTalk.IsAtAll,
		)
		notifierManager.Register("dingtalk", dingtalkNotifier)
		setupLog.Info("Registered DingTalk notifier")
	}

	// 设置 AIIncident Controller
	if err = (&controllers.AIIncidentReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		LLMManager:      llmManager,
		NotifierManager: notifierManager,
		Config:          cfg,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create controller", "controller", "AIIncident")
		os.Exit(1)
	}

	if err = (&controllers.EventIncidentReconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		RestConfig: restCfg,
		Config:     cfg,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create controller", "controller", "EventIncident")
		os.Exit(1)
	}

	if err := mgr.Add(&inspection.Runner{
		Client:          mgr.GetClient(),
		RestConfig:      restCfg,
		LLMManager:      llmManager,
		NotifierManager: notifierManager,
		Config:          cfg,
	}); err != nil {
		setupLog.Error(err, "Unable to add inspection runner")
		os.Exit(1)
	}

	// 设置健康检查
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Unable to set up ready check")
		os.Exit(1)
	}

	// 优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		setupLog.Info("Received shutdown signal, shutting down gracefully...")
		cancel()
	}()

	notifierManager.Start(ctx)

	// 启动 Manager
	setupLog.Info("Starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "Problem running manager")
		os.Exit(1)
	}

	<-ctx.Done()
	setupLog.Info("Manager stopped gracefully")
}
