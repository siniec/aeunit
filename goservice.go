package aeunit

import (
	"appengine_internal"
	pb "appengine_internal/base"
	"fmt"
)

type goService struct {
}

func (this *goService) Call(method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	if method == "GetNamespace" || method == "GetDefaultNamespace" {
		outStr := out.(*pb.StringProto)
		s := ""
		outStr.Value = &s
		return nil
	} else {
		return fmt.Errorf("Unknown method for __go__ service: %s", method)
	}
}

func (this *goService) Close() {}
