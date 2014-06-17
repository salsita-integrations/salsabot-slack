// Copyright (c) 2013 The go-meeko AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package rpc

import (
	// Stdlib
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"

	// Meeko
	"github.com/meeko/go-meeko/meeko/services/rpc"
	"github.com/meeko/go-meeko/meeko/utils/codecs"
)

// rpc.RemoteRequest

type remoteRequest struct {
	t   *Transport
	msg [][]byte

	sender string
	id     rpc.RequestID
	method string
	stdout io.Writer
	stderr io.Writer

	interrupted chan struct{}
	resolved    chan struct{}
}

func newRequest(t *Transport, msg [][]byte) *remoteRequest {
	// Parse request ID.
	var id rpc.RequestID
	if err := binary.Read(bytes.NewReader(msg[3]), binary.BigEndian, &id); err != nil {
		panic(err)
	}

	// Set up stdout streaming.
	var stdoutWriter io.Writer
	if len(msg[6]) == 2 {
		stdoutWriter = &streamWriter{
			transport: t,
			receiver:  msg[0],
			tag:       msg[6],
		}

	} else {
		stdoutWriter = ioutil.Discard
	}

	// Set up stderr streaming.
	var stderrWriter io.Writer
	if len(msg[7]) == 2 {
		stderrWriter = &streamWriter{
			transport: t,
			receiver:  msg[0],
			tag:       msg[7],
		}
	} else {
		stderrWriter = ioutil.Discard
	}

	return &remoteRequest{
		t:           t,
		msg:         msg,
		sender:      string(msg[0]),
		id:          id,
		method:      string(msg[4]),
		stdout:      stdoutWriter,
		stderr:      stderrWriter,
		interrupted: make(chan struct{}),
		resolved:    make(chan struct{}),
	}
}

func (req *remoteRequest) Sender() string {
	return req.sender
}

func (req *remoteRequest) Id() rpc.RequestID {
	return req.id
}

func (req *remoteRequest) Method() string {
	return req.method
}

func (req *remoteRequest) UnmarshalArgs(dst interface{}) error {
	return codecs.MessagePack.Decode(bytes.NewReader(req.msg[5]), dst)
}

type signalProgressCmd struct {
	msg   [][]byte
	errCh chan error
}

func (cmd *signalProgressCmd) Type() int {
	return rpc.CmdSignalProgress
}

func (cmd *signalProgressCmd) ErrorChan() chan<- error {
	return cmd.errCh
}

func (req *remoteRequest) SignalProgress() error {
	errCh := make(chan error, 1)
	req.t.exec(&signalProgressCmd{
		msg: [][]byte{
			req.msg[0],
			frameHeader,
			frameProgressMT,
			req.msg[3],
		},
		errCh: errCh,
	})
	return <-errCh
}

func (req *remoteRequest) Stdout() io.Writer {
	return req.stdout
}

func (req *remoteRequest) Stderr() io.Writer {
	return req.stderr
}

func (req *remoteRequest) Interrupted() <-chan struct{} {
	return req.interrupted
}

type replyCmd struct {
	msg   [][]byte
	errCh chan error
}

func (cmd *replyCmd) Type() int {
	return rpc.CmdReply
}

func (cmd *replyCmd) ErrorChan() chan<- error {
	return cmd.errCh
}

func (req *remoteRequest) Resolve(returnCode rpc.ReturnCode, returnValue interface{}) error {
	var valueBuffer bytes.Buffer
	if err := codecs.MessagePack.Encode(&valueBuffer, returnValue); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	req.t.exec(&replyCmd{
		msg: [][]byte{
			req.msg[0],
			frameHeader,
			frameReplyMT,
			req.msg[3],
			[]byte{byte(returnCode)},
			valueBuffer.Bytes(),
		},
		errCh: errCh,
	})
	if err := <-errCh; err != nil {
		return err
	}

	close(req.resolved)
	return nil
}

func (req *remoteRequest) Resolved() <-chan struct{} {
	return req.resolved
}

func (req *remoteRequest) interrupt() {
	select {
	case <-req.interrupted:
	default:
		close(req.interrupted)
	}
}

type streamWriter struct {
	transport *Transport
	receiver  []byte
	tag       []byte
}

type sendStreamFrameCmd struct {
	msg   [][]byte
	errCh chan error
}

func (cmd *sendStreamFrameCmd) Type() int {
	return rpc.CmdSendStreamFrame
}

func (cmd *sendStreamFrameCmd) ErrorChan() chan<- error {
	return cmd.errCh
}

func (w *streamWriter) Write(p []byte) (n int, err error) {
	errCh := make(chan error, 1)
	w.transport.exec(&sendStreamFrameCmd{
		msg: [][]byte{
			w.receiver,
			frameHeader,
			frameStreamFrameMT,
			w.tag,
			p,
		},
		errCh: errCh,
	})
	if err := <-errCh; err != nil {
		return 0, err
	}
	return len(p), nil
}

// rpc.StreamFrame -------------------------------------------------------------

type streamFrame [][]byte

func newStreamFrame(msg [][]byte) rpc.StreamFrame {
	return streamFrame(msg)
}

func (frame streamFrame) TargetStreamTag() rpc.StreamTag {
	msg := [][]byte(frame)
	var tag rpc.StreamTag
	if err := binary.Read(bytes.NewReader(msg[3]), binary.BigEndian, &tag); err != nil {
		panic(err)
	}
	return tag
}

func (frame streamFrame) Payload() []byte {
	msg := [][]byte(frame)
	return msg[4]
}

// rpc.RemoteCallReply ---------------------------------------------------------

type remoteCallReply [][]byte

func newReply(msg [][]byte) rpc.RemoteCallReply {
	return remoteCallReply(msg)
}

func (reply remoteCallReply) TargetCallId() rpc.RequestID {
	msg := [][]byte(reply)
	var id rpc.RequestID
	if err := binary.Read(bytes.NewReader(msg[3]), binary.BigEndian, &id); err != nil {
		panic(err)
	}
	return id
}

func (reply remoteCallReply) ReturnCode() rpc.ReturnCode {
	msg := [][]byte(reply)
	return rpc.ReturnCode(msg[4][0])
}

func (reply remoteCallReply) UnmarshalReturnValue(dst interface{}) error {
	msg := [][]byte(reply)
	return codecs.MessagePack.Decode(bytes.NewReader(msg[5]), dst)
}

// Errors ----------------------------------------------------------------------

var ErrResolved = errors.New("request already resolved")
