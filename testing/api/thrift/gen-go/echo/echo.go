// Code generated by Thrift Compiler (0.19.0). DO NOT EDIT.

package echo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	thrift "github.com/apache/thrift/lib/go/thrift"
	"regexp"
	"strings"
	"time"
)

// (needed to ensure safety because of naive import list construction.)
var _ = thrift.ZERO
var _ = fmt.Printf
var _ = errors.New
var _ = context.Background
var _ = time.Now
var _ = bytes.Equal

// (needed by validator.)
var _ = strings.Contains
var _ = regexp.MatchString

// Attributes:
//  - Msg
type Request struct {
	Msg string `thrift:"Msg,1" db:"Msg" json:"Msg"`
}

func NewRequest() *Request {
	return &Request{}
}

func (p *Request) GetMsg() string {
	return p.Msg
}
func (p *Request) Read(ctx context.Context, iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin(ctx)
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 1:
			if fieldTypeId == thrift.STRING {
				if err := p.ReadField1(ctx, iprot); err != nil {
					return err
				}
			} else {
				if err := iprot.Skip(ctx, fieldTypeId); err != nil {
					return err
				}
			}
		default:
			if err := iprot.Skip(ctx, fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(ctx); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

func (p *Request) ReadField1(ctx context.Context, iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(ctx); err != nil {
		return thrift.PrependError("error reading field 1: ", err)
	} else {
		p.Msg = v
	}
	return nil
}

func (p *Request) Write(ctx context.Context, oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin(ctx, "Request"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if p != nil {
		if err := p.writeField1(ctx, oprot); err != nil {
			return err
		}
	}
	if err := oprot.WriteFieldStop(ctx); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := oprot.WriteStructEnd(ctx); err != nil {
		return thrift.PrependError("write struct stop error: ", err)
	}
	return nil
}

func (p *Request) writeField1(ctx context.Context, oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin(ctx, "Msg", thrift.STRING, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:Msg: ", p), err)
	}
	if err := oprot.WriteString(ctx, string(p.Msg)); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T.Msg (1) field write error: ", p), err)
	}
	if err := oprot.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:Msg: ", p), err)
	}
	return err
}

func (p *Request) Equals(other *Request) bool {
	if p == other {
		return true
	} else if p == nil || other == nil {
		return false
	}
	if p.Msg != other.Msg {
		return false
	}
	return true
}

func (p *Request) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Request(%+v)", *p)
}

func (p *Request) Validate() error {
	return nil
}

// Attributes:
//  - Msg
type Response struct {
	Msg string `thrift:"Msg,1" db:"Msg" json:"Msg"`
}

func NewResponse() *Response {
	return &Response{}
}

func (p *Response) GetMsg() string {
	return p.Msg
}
func (p *Response) Read(ctx context.Context, iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin(ctx)
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 1:
			if fieldTypeId == thrift.STRING {
				if err := p.ReadField1(ctx, iprot); err != nil {
					return err
				}
			} else {
				if err := iprot.Skip(ctx, fieldTypeId); err != nil {
					return err
				}
			}
		default:
			if err := iprot.Skip(ctx, fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(ctx); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

func (p *Response) ReadField1(ctx context.Context, iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(ctx); err != nil {
		return thrift.PrependError("error reading field 1: ", err)
	} else {
		p.Msg = v
	}
	return nil
}

func (p *Response) Write(ctx context.Context, oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin(ctx, "Response"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if p != nil {
		if err := p.writeField1(ctx, oprot); err != nil {
			return err
		}
	}
	if err := oprot.WriteFieldStop(ctx); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := oprot.WriteStructEnd(ctx); err != nil {
		return thrift.PrependError("write struct stop error: ", err)
	}
	return nil
}

func (p *Response) writeField1(ctx context.Context, oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin(ctx, "Msg", thrift.STRING, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:Msg: ", p), err)
	}
	if err := oprot.WriteString(ctx, string(p.Msg)); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T.Msg (1) field write error: ", p), err)
	}
	if err := oprot.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:Msg: ", p), err)
	}
	return err
}

func (p *Response) Equals(other *Response) bool {
	if p == other {
		return true
	} else if p == nil || other == nil {
		return false
	}
	if p.Msg != other.Msg {
		return false
	}
	return true
}

func (p *Response) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Response(%+v)", *p)
}

func (p *Response) Validate() error {
	return nil
}

type EchoService interface {
	// Parameters:
	//  - Req
	Echo(ctx context.Context, req *Request) (_r *Response, _err error)
	// Parameters:
	//  - Req
	VisitOneway(ctx context.Context, req *Request) (_err error)
}

type EchoServiceClient struct {
	c    thrift.TClient
	meta thrift.ResponseMeta
}

func NewEchoServiceClientFactory(t thrift.TTransport, f thrift.TProtocolFactory) *EchoServiceClient {
	return &EchoServiceClient{
		c: thrift.NewTStandardClient(f.GetProtocol(t), f.GetProtocol(t)),
	}
}

func NewEchoServiceClientProtocol(t thrift.TTransport, iprot thrift.TProtocol, oprot thrift.TProtocol) *EchoServiceClient {
	return &EchoServiceClient{
		c: thrift.NewTStandardClient(iprot, oprot),
	}
}

func NewEchoServiceClient(c thrift.TClient) *EchoServiceClient {
	return &EchoServiceClient{
		c: c,
	}
}

func (p *EchoServiceClient) Client_() thrift.TClient {
	return p.c
}

func (p *EchoServiceClient) LastResponseMeta_() thrift.ResponseMeta {
	return p.meta
}

func (p *EchoServiceClient) SetLastResponseMeta_(meta thrift.ResponseMeta) {
	p.meta = meta
}

// Parameters:
//  - Req
func (p *EchoServiceClient) Echo(ctx context.Context, req *Request) (_r *Response, _err error) {
	var _args0 EchoServiceEchoArgs
	_args0.Req = req
	var _result2 EchoServiceEchoResult
	var _meta1 thrift.ResponseMeta
	_meta1, _err = p.Client_().Call(ctx, "Echo", &_args0, &_result2)
	p.SetLastResponseMeta_(_meta1)
	if _err != nil {
		return
	}
	if _ret3 := _result2.GetSuccess(); _ret3 != nil {
		return _ret3, nil
	}
	return nil, thrift.NewTApplicationException(thrift.MISSING_RESULT, "Echo failed: unknown result")
}

// Parameters:
//  - Req
func (p *EchoServiceClient) VisitOneway(ctx context.Context, req *Request) (_err error) {
	var _args4 EchoServiceVisitOnewayArgs
	_args4.Req = req
	p.SetLastResponseMeta_(thrift.ResponseMeta{})
	if _, err := p.Client_().Call(ctx, "VisitOneway", &_args4, nil); err != nil {
		return err
	}
	return nil
}

type EchoServiceProcessor struct {
	processorMap map[string]thrift.TProcessorFunction
	handler      EchoService
}

func (p *EchoServiceProcessor) AddToProcessorMap(key string, processor thrift.TProcessorFunction) {
	p.processorMap[key] = processor
}

func (p *EchoServiceProcessor) GetProcessorFunction(key string) (processor thrift.TProcessorFunction, ok bool) {
	processor, ok = p.processorMap[key]
	return processor, ok
}

func (p *EchoServiceProcessor) ProcessorMap() map[string]thrift.TProcessorFunction {
	return p.processorMap
}

func NewEchoServiceProcessor(handler EchoService) *EchoServiceProcessor {

	self5 := &EchoServiceProcessor{handler: handler, processorMap: make(map[string]thrift.TProcessorFunction)}
	self5.processorMap["Echo"] = &echoServiceProcessorEcho{handler: handler}
	self5.processorMap["VisitOneway"] = &echoServiceProcessorVisitOneway{handler: handler}
	return self5
}

func (p *EchoServiceProcessor) Process(ctx context.Context, iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
	name, _, seqId, err2 := iprot.ReadMessageBegin(ctx)
	if err2 != nil {
		return false, thrift.WrapTException(err2)
	}
	if processor, ok := p.GetProcessorFunction(name); ok {
		return processor.Process(ctx, seqId, iprot, oprot)
	}
	iprot.Skip(ctx, thrift.STRUCT)
	iprot.ReadMessageEnd(ctx)
	x6 := thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "Unknown function "+name)
	oprot.WriteMessageBegin(ctx, name, thrift.EXCEPTION, seqId)
	x6.Write(ctx, oprot)
	oprot.WriteMessageEnd(ctx)
	oprot.Flush(ctx)
	return false, x6

}

type echoServiceProcessorEcho struct {
	handler EchoService
}

func (p *echoServiceProcessorEcho) Process(ctx context.Context, seqId int32, iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
	var _write_err7 error
	args := EchoServiceEchoArgs{}
	if err2 := args.Read(ctx, iprot); err2 != nil {
		iprot.ReadMessageEnd(ctx)
		x := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, err2.Error())
		oprot.WriteMessageBegin(ctx, "Echo", thrift.EXCEPTION, seqId)
		x.Write(ctx, oprot)
		oprot.WriteMessageEnd(ctx)
		oprot.Flush(ctx)
		return false, thrift.WrapTException(err2)
	}
	iprot.ReadMessageEnd(ctx)

	tickerCancel := func() {}
	// Start a goroutine to do server side connectivity check.
	if thrift.ServerConnectivityCheckInterval > 0 {
		var cancel context.CancelCauseFunc
		ctx, cancel = context.WithCancelCause(ctx)
		defer cancel(nil)
		var tickerCtx context.Context
		tickerCtx, tickerCancel = context.WithCancel(context.Background())
		defer tickerCancel()
		go func(ctx context.Context, cancel context.CancelCauseFunc) {
			ticker := time.NewTicker(thrift.ServerConnectivityCheckInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if !iprot.Transport().IsOpen() {
						cancel(thrift.ErrAbandonRequest)
						return
					}
				}
			}
		}(tickerCtx, cancel)
	}

	result := EchoServiceEchoResult{}
	if retval, err2 := p.handler.Echo(ctx, args.Req); err2 != nil {
		tickerCancel()
		err = thrift.WrapTException(err2)
		if errors.Is(err2, thrift.ErrAbandonRequest) {
			return false, thrift.WrapTException(err2)
		}
		if errors.Is(err2, context.Canceled) {
			if err := context.Cause(ctx); errors.Is(err, thrift.ErrAbandonRequest) {
				return false, thrift.WrapTException(err)
			}
		}
		_exc8 := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "Internal error processing Echo: "+err2.Error())
		if err2 := oprot.WriteMessageBegin(ctx, "Echo", thrift.EXCEPTION, seqId); err2 != nil {
			_write_err7 = thrift.WrapTException(err2)
		}
		if err2 := _exc8.Write(ctx, oprot); _write_err7 == nil && err2 != nil {
			_write_err7 = thrift.WrapTException(err2)
		}
		if err2 := oprot.WriteMessageEnd(ctx); _write_err7 == nil && err2 != nil {
			_write_err7 = thrift.WrapTException(err2)
		}
		if err2 := oprot.Flush(ctx); _write_err7 == nil && err2 != nil {
			_write_err7 = thrift.WrapTException(err2)
		}
		if _write_err7 != nil {
			return false, thrift.WrapTException(_write_err7)
		}
		return true, err
	} else {
		result.Success = retval
	}
	tickerCancel()
	if err2 := oprot.WriteMessageBegin(ctx, "Echo", thrift.REPLY, seqId); err2 != nil {
		_write_err7 = thrift.WrapTException(err2)
	}
	if err2 := result.Write(ctx, oprot); _write_err7 == nil && err2 != nil {
		_write_err7 = thrift.WrapTException(err2)
	}
	if err2 := oprot.WriteMessageEnd(ctx); _write_err7 == nil && err2 != nil {
		_write_err7 = thrift.WrapTException(err2)
	}
	if err2 := oprot.Flush(ctx); _write_err7 == nil && err2 != nil {
		_write_err7 = thrift.WrapTException(err2)
	}
	if _write_err7 != nil {
		return false, thrift.WrapTException(_write_err7)
	}
	return true, err
}

type echoServiceProcessorVisitOneway struct {
	handler EchoService
}

func (p *echoServiceProcessorVisitOneway) Process(ctx context.Context, seqId int32, iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
	args := EchoServiceVisitOnewayArgs{}
	if err2 := args.Read(ctx, iprot); err2 != nil {
		iprot.ReadMessageEnd(ctx)
		return false, thrift.WrapTException(err2)
	}
	iprot.ReadMessageEnd(ctx)

	tickerCancel := func() {}
	_ = tickerCancel

	if err2 := p.handler.VisitOneway(ctx, args.Req); err2 != nil {
		tickerCancel()
		err = thrift.WrapTException(err2)
	}
	tickerCancel()
	return true, err
}

// HELPER FUNCTIONS AND STRUCTURES

// Attributes:
//  - Req
type EchoServiceEchoArgs struct {
	Req *Request `thrift:"req,1" db:"req" json:"req"`
}

func NewEchoServiceEchoArgs() *EchoServiceEchoArgs {
	return &EchoServiceEchoArgs{}
}

var EchoServiceEchoArgs_Req_DEFAULT *Request

func (p *EchoServiceEchoArgs) GetReq() *Request {
	if !p.IsSetReq() {
		return EchoServiceEchoArgs_Req_DEFAULT
	}
	return p.Req
}
func (p *EchoServiceEchoArgs) IsSetReq() bool {
	return p.Req != nil
}

func (p *EchoServiceEchoArgs) Read(ctx context.Context, iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin(ctx)
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 1:
			if fieldTypeId == thrift.STRUCT {
				if err := p.ReadField1(ctx, iprot); err != nil {
					return err
				}
			} else {
				if err := iprot.Skip(ctx, fieldTypeId); err != nil {
					return err
				}
			}
		default:
			if err := iprot.Skip(ctx, fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(ctx); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

func (p *EchoServiceEchoArgs) ReadField1(ctx context.Context, iprot thrift.TProtocol) error {
	p.Req = &Request{}
	if err := p.Req.Read(ctx, iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Req), err)
	}
	return nil
}

func (p *EchoServiceEchoArgs) Write(ctx context.Context, oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin(ctx, "Echo_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if p != nil {
		if err := p.writeField1(ctx, oprot); err != nil {
			return err
		}
	}
	if err := oprot.WriteFieldStop(ctx); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := oprot.WriteStructEnd(ctx); err != nil {
		return thrift.PrependError("write struct stop error: ", err)
	}
	return nil
}

func (p *EchoServiceEchoArgs) writeField1(ctx context.Context, oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin(ctx, "req", thrift.STRUCT, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:req: ", p), err)
	}
	if err := p.Req.Write(ctx, oprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Req), err)
	}
	if err := oprot.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:req: ", p), err)
	}
	return err
}

func (p *EchoServiceEchoArgs) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("EchoServiceEchoArgs(%+v)", *p)
}

// Attributes:
//  - Success
type EchoServiceEchoResult struct {
	Success *Response `thrift:"success,0" db:"success" json:"success,omitempty"`
}

func NewEchoServiceEchoResult() *EchoServiceEchoResult {
	return &EchoServiceEchoResult{}
}

var EchoServiceEchoResult_Success_DEFAULT *Response

func (p *EchoServiceEchoResult) GetSuccess() *Response {
	if !p.IsSetSuccess() {
		return EchoServiceEchoResult_Success_DEFAULT
	}
	return p.Success
}
func (p *EchoServiceEchoResult) IsSetSuccess() bool {
	return p.Success != nil
}

func (p *EchoServiceEchoResult) Read(ctx context.Context, iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin(ctx)
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 0:
			if fieldTypeId == thrift.STRUCT {
				if err := p.ReadField0(ctx, iprot); err != nil {
					return err
				}
			} else {
				if err := iprot.Skip(ctx, fieldTypeId); err != nil {
					return err
				}
			}
		default:
			if err := iprot.Skip(ctx, fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(ctx); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

func (p *EchoServiceEchoResult) ReadField0(ctx context.Context, iprot thrift.TProtocol) error {
	p.Success = &Response{}
	if err := p.Success.Read(ctx, iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Success), err)
	}
	return nil
}

func (p *EchoServiceEchoResult) Write(ctx context.Context, oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin(ctx, "Echo_result"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if p != nil {
		if err := p.writeField0(ctx, oprot); err != nil {
			return err
		}
	}
	if err := oprot.WriteFieldStop(ctx); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := oprot.WriteStructEnd(ctx); err != nil {
		return thrift.PrependError("write struct stop error: ", err)
	}
	return nil
}

func (p *EchoServiceEchoResult) writeField0(ctx context.Context, oprot thrift.TProtocol) (err error) {
	if p.IsSetSuccess() {
		if err := oprot.WriteFieldBegin(ctx, "success", thrift.STRUCT, 0); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 0:success: ", p), err)
		}
		if err := p.Success.Write(ctx, oprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Success), err)
		}
		if err := oprot.WriteFieldEnd(ctx); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 0:success: ", p), err)
		}
	}
	return err
}

func (p *EchoServiceEchoResult) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("EchoServiceEchoResult(%+v)", *p)
}

// Attributes:
//  - Req
type EchoServiceVisitOnewayArgs struct {
	Req *Request `thrift:"req,1" db:"req" json:"req"`
}

func NewEchoServiceVisitOnewayArgs() *EchoServiceVisitOnewayArgs {
	return &EchoServiceVisitOnewayArgs{}
}

var EchoServiceVisitOnewayArgs_Req_DEFAULT *Request

func (p *EchoServiceVisitOnewayArgs) GetReq() *Request {
	if !p.IsSetReq() {
		return EchoServiceVisitOnewayArgs_Req_DEFAULT
	}
	return p.Req
}
func (p *EchoServiceVisitOnewayArgs) IsSetReq() bool {
	return p.Req != nil
}

func (p *EchoServiceVisitOnewayArgs) Read(ctx context.Context, iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin(ctx)
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 1:
			if fieldTypeId == thrift.STRUCT {
				if err := p.ReadField1(ctx, iprot); err != nil {
					return err
				}
			} else {
				if err := iprot.Skip(ctx, fieldTypeId); err != nil {
					return err
				}
			}
		default:
			if err := iprot.Skip(ctx, fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(ctx); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

func (p *EchoServiceVisitOnewayArgs) ReadField1(ctx context.Context, iprot thrift.TProtocol) error {
	p.Req = &Request{}
	if err := p.Req.Read(ctx, iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Req), err)
	}
	return nil
}

func (p *EchoServiceVisitOnewayArgs) Write(ctx context.Context, oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin(ctx, "VisitOneway_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if p != nil {
		if err := p.writeField1(ctx, oprot); err != nil {
			return err
		}
	}
	if err := oprot.WriteFieldStop(ctx); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := oprot.WriteStructEnd(ctx); err != nil {
		return thrift.PrependError("write struct stop error: ", err)
	}
	return nil
}

func (p *EchoServiceVisitOnewayArgs) writeField1(ctx context.Context, oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin(ctx, "req", thrift.STRUCT, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:req: ", p), err)
	}
	if err := p.Req.Write(ctx, oprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Req), err)
	}
	if err := oprot.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:req: ", p), err)
	}
	return err
}

func (p *EchoServiceVisitOnewayArgs) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("EchoServiceVisitOnewayArgs(%+v)", *p)
}
