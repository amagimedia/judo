// Code generated by mockery v1.0.0. DO NOT EDIT.
package mocks

import mock "github.com/stretchr/testify/mock"
import nats "github.com/nats-io/go-nats"
import stan "github.com/nats-io/go-nats-streaming"

// RawConnection is an autogenerated mock type for the RawConnection type
type RawConnection struct {
	mock.Mock
}

// ChanSubscribe provides a mock function with given fields: _a0, _a1
func (_m RawConnection) ChanSubscribe(_a0 string, _a1 interface{}) (*nats.Subscription, error) {
	ret := _m.Called(_a0, _a1)

	var r0 *nats.Subscription
	if rf, ok := ret.Get(0).(func(string, interface{}) *nats.Subscription); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*nats.Subscription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, interface{}) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Close provides a mock function with given fields:
func (_m RawConnection) Close() {
	_m.Called()
}

// Publish provides a mock function with given fields: _a0, _a1
func (_m RawConnection) Publish(_a0 string, _a1 []byte) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, []byte) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Subscribe provides a mock function with given fields: _a0, _a1, _a2
func (_m RawConnection) Subscribe(_a0 string, _a1 stan.MsgHandler, _a2 ...stan.SubscriptionOption) (stan.Subscription, error) {
	_va := make([]interface{}, len(_a2))
	for _i := range _a2 {
		_va[_i] = _a2[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _a0, _a1)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 stan.Subscription
	if rf, ok := ret.Get(0).(func(string, stan.MsgHandler, ...stan.SubscriptionOption) stan.Subscription); ok {
		r0 = rf(_a0, _a1, _a2...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(stan.Subscription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, stan.MsgHandler, ...stan.SubscriptionOption) error); ok {
		r1 = rf(_a0, _a1, _a2...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
