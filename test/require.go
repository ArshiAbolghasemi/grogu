package test

import (
	"reflect"
	"testing"
)

func NoError(t *testing.T, err error, msgAndArgs ...any) {
	t.Helper()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func NotNil(t *testing.T, object any, msgAndArgs ...any) {
	t.Helper()

	if object == nil || (reflect.ValueOf(object).Kind() == reflect.Ptr && reflect.ValueOf(object).IsNil()) {
		t.Fatalf("expected value to be non-nil")
	}
}

func Equal(t *testing.T, expected, actual any, msgAndArgs ...any) {
	t.Helper()

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}

func True(t *testing.T, value bool, msgAndArgs ...any) {
	t.Helper()

	if !value {
		t.Fatalf("expected condition to be true")
	}
}
