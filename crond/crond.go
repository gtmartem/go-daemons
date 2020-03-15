package crond

import (
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"os/signal"
	"syscall"
)


// Cron is base structure for cron daemon app
type Cron struct {
	cron		*cron.Cron
	logger 		*logrus.Logger
	config		*cronConfig
}


// NewCron prepares and return new cron daemon instance
func NewCron(pathToConfig string, function func()) *Cron {
	config := getConfig(pathToConfig)
	c := cron.New()
	_, err := c.AddFunc(config.Cron, function)
	if err != nil {
		log.Fatalf("err during adding function to cron: %s", err)
	}
	daemon := &Cron{
		cron: c,
		config: config,
	}
	daemon.setupLogger()
	return daemon
}


func (c *Cron) Start() {
	c.logger.Info("starting cron-daemon work")
	c.cron.Start()
	cronShutdownCatcher := make(chan os.Signal, 1)
	go c.shutdown(cronShutdownCatcher)
	c.cronShutdownChecker(cronShutdownCatcher)
}


func (c *Cron) shutdown(cronShutdownCatcher chan os.Signal) {
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	select {
	case sig := <-shutdownChan:
		<- c.cron.Stop().Done()
		cronShutdownCatcher <- sig
	}
}


func (c *Cron) cronShutdownChecker(cronShutdownCatcher chan os.Signal) {
	<- cronShutdownCatcher
	c.logger.Info("cron-daemon shutdown")
}


// Creates and returns default logger with level INFO
func (c *Cron) setupLogger() (logger *logrus.Logger) {
	logger = logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		ForceColors:               true,
		FullTimestamp:             true,
		TimestampFormat:           "2006-01-02 15:04:05",
	})
	logLevel, err := logrus.ParseLevel(c.config.LogLevel)
	if err != nil {
		log.Fatalf("incorrect logger level: %s", err)
	}
	logger.SetLevel(logLevel)
	return
}
