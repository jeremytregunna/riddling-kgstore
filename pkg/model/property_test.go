package model

import (
	"testing"
)

func TestNewProperty(t *testing.T) {
	key := "name"
	value := "John Doe"

	property := NewProperty(key, value)

	if property.Key != key {
		t.Errorf("Expected property key to be %s, got %s", key, property.Key)
	}

	if property.Value != value {
		t.Errorf("Expected property value to be %s, got %s", value, property.Value)
	}
}

func TestPropertyAccessors(t *testing.T) {
	property := NewProperty("age", "30")

	if property.GetKey() != "age" {
		t.Errorf("Expected GetKey() to return 'age', got '%s'", property.GetKey())
	}

	if property.GetValue() != "30" {
		t.Errorf("Expected GetValue() to return '30', got '%s'", property.GetValue())
	}
}

func TestPropertySetValue(t *testing.T) {
	property := NewProperty("age", "30")

	property.SetValue("31")

	if property.Value != "31" {
		t.Errorf("Expected value to be '31' after SetValue, got '%s'", property.Value)
	}

	if property.GetValue() != "31" {
		t.Errorf("Expected GetValue() to return '31' after SetValue, got '%s'", property.GetValue())
	}
}
