package aeunit

import (
	"appengine_internal"
	"fmt"
	"github.com/siniec/aeunit/datastore"
)

type LogLevel int

const (
	LogLevelDebug    LogLevel = 5
	LogLevelInfo              = 4
	LogLevelWarning           = 3
	LogLevelError             = 2
	LogLevelCritical          = 1
	LogLevelNone              = 0
)

type Service interface {
	Call(method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error
	Close() error
}

type ContextOptions struct {
}

type Logger interface {
	Logf(lvl LogLevel, s string, v ...interface{})
}

type defaultLogger struct{}

func (this *defaultLogger) Logf(lvl LogLevel, s string, v ...interface{}) {
	fmt.Printf(s+"\n", v...)
}

type Context struct {
	opt      ContextOptions
	services map[string]Service
	logger   Logger
	appID    string
}

func (this *Context) Close() error {
	var err error
	for _, s := range this.services {
		if e := s.Close(); e != nil {
			err = e
		}
	}
	return err
}

func NewContext(opt *ContextOptions) *Context {
	if opt == nil {
		opt = &ContextOptions{}
	}
	c := &Context{opt: *opt, services: make(map[string]Service), appID: "dev~aeunit"}
	c.SetService("__go__", &goService{})
	c.SetService("datastore_v3", datastore.New())
	c.logger = &defaultLogger{}
	return c
}

func (this *Context) SetService(name string, service Service) {
	this.services[name] = service
}

func (this *Context) SetLogger(logger Logger) {
	this.logger = logger
}

func (this *Context) logf(lvl LogLevel, s string, v ...interface{}) {
	if this.logger != nil {
		this.logger.Logf(lvl, s, v...)
	}
}

func (this *Context) Debugf(s string, v ...interface{})    { this.logf(LogLevelDebug, s, v...) }
func (this *Context) Infof(s string, v ...interface{})     { this.logf(LogLevelInfo, s, v...) }
func (this *Context) Warningf(s string, v ...interface{})  { this.logf(LogLevelWarning, s, v...) }
func (this *Context) Errorf(s string, v ...interface{})    { this.logf(LogLevelError, s, v...) }
func (this *Context) Criticalf(s string, v ...interface{}) { this.logf(LogLevelCritical, s, v...) }

func (this *Context) Call(service, method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	if s, ok := this.services[service]; ok {
		return s.Call(method, in, out, opts)
	} else {
		return fmt.Errorf("Unknown service: %s", service)
	}
}

func (this *Context) FullyQualifiedAppID() string {
	return this.appID
}

func (this *Context) SetFullyQualifiedAppID(id string) {
	this.appID = id
}

func (this *Context) Request() interface{} {
	panic("Request() is not implemented")
}
