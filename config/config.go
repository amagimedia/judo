package config

import (
	"errors"
	"github.com/streadway/amqp"
	"reflect"
)

type ConfigHelper struct {
	Config
}

type Config interface {
	GetKeys() []string
	GetMandatoryKeys() []string
	GetField(string) string
}

func (c ConfigHelper) set(key string, val interface{}) error {
	var err error
	err = nil
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("Invalid Type found for config " + key)
		}
	}()

	fieldName := c.GetField(key)

	e := reflect.ValueOf(c.Config).Elem()

	if e.Kind() != reflect.Struct {
		return errors.New("Not a struct type")
	}

	field := e.FieldByName(fieldName)
	if !field.IsValid() {
		return errors.New("Invalid field key " + fieldName)
	}

	switch val.(type) {
	case string:
		if field.CanSet() {
			field.SetString(val.(string))
		}
	case []string:
		if field.CanSet() {
			field.Set(reflect.ValueOf(val.([]string)))
		}
	case bool:
		if field.CanSet() {
			field.SetBool(val.(bool))
		}
	case nil:
		if field.CanSet() {
			field.Set(reflect.ValueOf(val.(amqp.Table)))
		}
	default:
		return errors.New("Unknown Type for " + fieldName)
	}

	return err

}

func (c ConfigHelper) ValidateAndSet(cfg map[string]interface{}) error {

	var err error
	allKeys := c.GetKeys()

	for _, key := range allKeys {
		switch val, ok := cfg[key]; ok {
		case true:
			err = c.set(key, val)
			if err != nil {
				return err
			}
		case false:
			if stringInSlice(key, c.GetMandatoryKeys()) {
				err = errors.New("Key Missing : " + key)
				return err
			}
		}
	}

	return err

}

func stringInSlice(value string, slice []string) bool {
	for _, val := range slice {
		if val == value {
			return true
		}
	}
	return false
}
