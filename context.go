package aeunit

import (
	"appengine_internal"
	"fmt"
	"github.com/siniec/aeunit/datastore"
)

type LogLvl int

const (
	LogLvlDebug    LogLvl = 5
	LogLvlInfo            = 4
	LogLvlWarning         = 3
	LogLvlError           = 2
	LogLvlCritical        = 1
	LogLvlNone            = 0
)

type Service interface {
	Call(method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error
	Close() error
}

type ContextOptions struct {
	LogLevel LogLvl
}

var defaultContextOptions = ContextOptions{}

type Context struct {
	opt      ContextOptions
	services map[string]Service
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
		opt = &defaultContextOptions
	}
	c := &Context{opt: *opt, services: make(map[string]Service)}
	c.SetService("__go__", &goService{})
	c.SetService("datastore_v3", datastore.New())
	return c
}

func (this *Context) SetService(name string, service Service) {
	this.services[name] = service
}

// TODO: make a logger interface, so we can change it, and make a deaultLogger struct
func (this *Context) logf(lvl LogLvl, s string, v ...interface{}) {
	if this.opt.LogLevel >= lvl {
		fmt.Printf(s+"\n", v...)
	}
}

func (this *Context) Debugf(s string, v ...interface{})    { this.logf(LogLvlDebug, s, v...) }
func (this *Context) Infof(s string, v ...interface{})     { this.logf(LogLvlInfo, s, v...) }
func (this *Context) Warningf(s string, v ...interface{})  { this.logf(LogLvlWarning, s, v...) }
func (this *Context) Errorf(s string, v ...interface{})    { this.logf(LogLvlError, s, v...) }
func (this *Context) Criticalf(s string, v ...interface{}) { this.logf(LogLvlCritical, s, v...) }

func (this *Context) Call(service, method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	if s, ok := this.services[service]; ok {
		return s.Call(method, in, out, opts)
	} else {
		return fmt.Errorf("Unknown service: %s", service)
	}
}

func (this *Context) FullyQualifiedAppID() string {
	return "dev~aeunit"
}

func (this *Context) Request() interface{} {
	panic("Request() is not implemented")
}
