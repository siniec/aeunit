package aeunit

import (
	"appengine_internal"
	"fmt"
	"github.com/siniec/aeunit/datastore"
)

type Service interface {
	Call(method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error
	Close()
}

type Context struct {
	services map[string]Service
}

func (this *Context) Close() {
	for _, s := range this.services {
		s.Close()
	}
}

func NewContext() *Context {
	c := &Context{services: make(map[string]Service)}
	c.SetService("__go__", &goService{})
	c.SetService("datastore_v3", datastore.New())
	return c
}

func (this *Context) SetService(name string, service Service) {
	this.services[name] = service
}

func (this *Context) Debugf(s string, v ...interface{}) {
	fmt.Printf(s, v...)
	fmt.Println()
}

func (this *Context) Infof(s string, v ...interface{}) {
	this.Debugf(s, v...)
}

func (this *Context) Warningf(s string, v ...interface{}) {
	this.Debugf(s, v...)
}

func (this *Context) Errorf(s string, v ...interface{}) {
	this.Debugf(s, v...)
}

func (this *Context) Criticalf(s string, v ...interface{}) {
	this.Debugf(s, v...)
}

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
