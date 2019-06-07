// Code generated by mockery v1.0.0. DO NOT EDIT.
package mocks

import message "github.com/amagimedia/judo/v2/message"
import mock "github.com/stretchr/testify/mock"

// RawMessage is an autogenerated mock type for the RawMessage type
type RawMessage struct {
	mock.Mock
}

// Ack provides a mock function with given fields: _a0
func (_m RawMessage) Ack(_a0 bool) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(bool) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetBody provides a mock function with given fields:
func (_m RawMessage) GetBody() []byte {
	ret := _m.Called()

	var r0 []byte
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	return r0
}

// GetCorrelationId provides a mock function with given fields:
func (_m RawMessage) GetCorrelationId() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// GetReplyTo provides a mock function with given fields:
func (_m RawMessage) GetReplyTo() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// Nack provides a mock function with given fields: _a0, _a1
func (_m RawMessage) Nack(_a0 bool, _a1 bool) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(bool, bool) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetBody provides a mock function with given fields: _a0
func (_m RawMessage) SetBody(_a0 []byte) message.RawMessage {
	ret := _m.Called(_a0)

	var r0 message.RawMessage
	if rf, ok := ret.Get(0).(func([]byte) message.RawMessage); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(message.RawMessage)
		}
	}

	return r0
}

// GetTimetoken provides a mock function returns: _r0
func (_m RawMessage) GetTimetoken() int64 {
	ret := _m.Called()

	var r0 int64
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	return r0
}
