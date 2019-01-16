package main

import "testing"

func TestFahrenheitToCelsius(t *testing.T) {
	v := fahrenheitToCelsius(-40.0)
	if v != -40 {
		t.Errorf("Expected -40, got %f", v)
	}
	v = fahrenheitToCelsius(41.0)
	if v != 5 {
		t.Errorf("Expected 5, got %f", v)
	}
	v = fahrenheitToCelsius(50.0)
	if v != 10 {
		t.Errorf("Expected 10, got %f", v)
	}
}

func TestMilesToKilometers(t *testing.T) {
	v := milesTokilometers(1.0)
	if v != 1.60934 {
		t.Errorf("Expected 1.60934, got %f", v)
	}
	v = milesTokilometers(23)
	if v != 37.014820 {
		t.Errorf("Expected 37.014820, got %f", v)
	}
}
