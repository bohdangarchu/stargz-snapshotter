/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// Package refreshtoc provides a gRPC service for dynamically refreshing
// external TOC layers while containers are running.
package refreshtoc

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RefreshTOCRequest is the request message for the Refresh RPC.
type RefreshTOCRequest struct {
	ImageRef string `protobuf:"bytes,1,opt,name=image_ref,json=imageRef,proto3" json:"image_ref,omitempty"`
}

func (m *RefreshTOCRequest) Reset()         { *m = RefreshTOCRequest{} }
func (m *RefreshTOCRequest) String() string { return proto.CompactTextString(m) }
func (*RefreshTOCRequest) ProtoMessage()    {}

func (m *RefreshTOCRequest) Marshal() ([]byte, error) {
	return proto.Marshal(m)
}

func (m *RefreshTOCRequest) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, m)
}

func (m *RefreshTOCRequest) GetImageRef() string {
	if m != nil {
		return m.ImageRef
	}
	return ""
}

// RefreshTOCResponse is the response message for the Refresh RPC.
type RefreshTOCResponse struct {
	LayersRefreshed int32  `protobuf:"varint,1,opt,name=layers_refreshed,json=layersRefreshed,proto3" json:"layers_refreshed,omitempty"`
	Message         string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (m *RefreshTOCResponse) Reset()         { *m = RefreshTOCResponse{} }
func (m *RefreshTOCResponse) String() string { return proto.CompactTextString(m) }
func (*RefreshTOCResponse) ProtoMessage()    {}

func (m *RefreshTOCResponse) Marshal() ([]byte, error) {
	return proto.Marshal(m)
}

func (m *RefreshTOCResponse) Unmarshal(b []byte) error {
	return proto.Unmarshal(b, m)
}

func (m *RefreshTOCResponse) GetLayersRefreshed() int32 {
	if m != nil {
		return m.LayersRefreshed
	}
	return 0
}

func (m *RefreshTOCResponse) GetMessage() string {
	if m != nil {
		return m.Message
	}
	return ""
}

func init() {
	proto.RegisterType((*RefreshTOCRequest)(nil), "stargz.refreshtoc.RefreshTOCRequest")
	proto.RegisterType((*RefreshTOCResponse)(nil), "stargz.refreshtoc.RefreshTOCResponse")
}

// RefreshTOCServiceServer is the server API for RefreshTOCService.
type RefreshTOCServiceServer interface {
	Refresh(context.Context, *RefreshTOCRequest) (*RefreshTOCResponse, error)
}

// UnimplementedRefreshTOCServiceServer can be embedded to have forward compatible implementations.
type UnimplementedRefreshTOCServiceServer struct{}

func (*UnimplementedRefreshTOCServiceServer) Refresh(ctx context.Context, req *RefreshTOCRequest) (*RefreshTOCResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Refresh not implemented")
}

// RegisterRefreshTOCServiceServer registers the service with the gRPC server.
func RegisterRefreshTOCServiceServer(s *grpc.Server, srv RefreshTOCServiceServer) {
	s.RegisterService(&_RefreshTOCService_serviceDesc, srv)
}

func _RefreshTOCService_Refresh_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RefreshTOCRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RefreshTOCServiceServer).Refresh(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/stargz.refreshtoc.RefreshTOCService/Refresh",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RefreshTOCServiceServer).Refresh(ctx, req.(*RefreshTOCRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _RefreshTOCService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "stargz.refreshtoc.RefreshTOCService",
	HandlerType: (*RefreshTOCServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Refresh",
			Handler:    _RefreshTOCService_Refresh_Handler,
		},
	},
	Streams: []grpc.StreamDesc{},
}

// RefreshTOCServiceClient is the client API for RefreshTOCService.
type RefreshTOCServiceClient interface {
	Refresh(ctx context.Context, in *RefreshTOCRequest, opts ...grpc.CallOption) (*RefreshTOCResponse, error)
}

type refreshTOCServiceClient struct {
	cc *grpc.ClientConn
}

// NewRefreshTOCServiceClient creates a new client for the RefreshTOCService.
func NewRefreshTOCServiceClient(cc *grpc.ClientConn) RefreshTOCServiceClient {
	return &refreshTOCServiceClient{cc}
}

func (c *refreshTOCServiceClient) Refresh(ctx context.Context, in *RefreshTOCRequest, opts ...grpc.CallOption) (*RefreshTOCResponse, error) {
	out := new(RefreshTOCResponse)
	err := c.cc.Invoke(ctx, "/stargz.refreshtoc.RefreshTOCService/Refresh", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
