// Code generated by mockery v1.0.0. DO NOT EDIT.
package mocks

import mock "github.com/stretchr/testify/mock"
import pubnub "github.com/pubnub/go"

// PubnubRawClient is an autogenerated mock type for the PubnubRawClient type
type PubnubRawClient struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *PubnubRawClient) Destroy(_a0 string) {
	_m.Called(_a0)
}

// EvalSha provides a mock function with given fields: _a0, _a1, _a2
func (_m *PubnubRawClient) FetchHistory(_a0 string, _a1 bool, _a2 int64, _a3 bool, _a4 int) ([]*pubnub.PNMessage, error) {
	ret := _m.Called(_a0, _a1, _a2, _a3, _a4)

	var r0 []*pubnub.PNMessage
	var r1 error
	if rf, ok := ret.Get(0).(func(string, bool, int64, bool, int) ([]*pubnub.PNMessage, error)); ok {
		r0, r1 = rf(_a0, _a1, _a2, _a3, _a4)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*pubnub.PNMessage)
		}
	}

	return r0, r1
}

// Publish provides a mock function with given fields: _a0, _a1
func (_m *PubnubRawClient) Publish(_a0 string, _a1 []byte) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(error)
		}
	}

	return r0
}

// ScriptLoad provides a mock function with given fields: _a0
func (_m *PubnubRawClient) AddListener(_a0 *pubnub.Listener) {
	ret := _m.Called(_a0)

	if rf, ok := ret.Get(0).(func(*pubnub.Listener)); ok {
		rf(_a0)
	} else {
		if ret.Get(0) != nil {
		}
	}

	return
}

// Subscribe provides a mock function with given fields: _a0
func (_m *PubnubRawClient) Subscribe(_a0 string) {
	ret := _m.Called(_a0)

	if rf, ok := ret.Get(0).(func(string)); ok {
		rf(_a0)
	} else {
		if ret.Get(0) != nil {
		}
	}

	return
}

func (_m *PubnubRawClient) GetListener() *pubnub.Listener {
	ret := _m.Called()

	var r0 *pubnub.Listener
	if rf, ok := ret.Get(0).(func() *pubnub.Listener); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pubnub.Listener)
		}
	}

	return r0
}
