package event

import (
	"encoding/json"
	"testing"
)

func TestOrderCreatedPayload_RoundTrip(t *testing.T) {
	orig := OrderCreatedPayload{OrderID: "ORD-001", Amount: 500000}

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got OrderCreatedPayload
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.OrderID != orig.OrderID {
		t.Errorf("OrderID = %q, want %q", got.OrderID, orig.OrderID)
	}
	if got.Amount != orig.Amount {
		t.Errorf("Amount = %v, want %v", got.Amount, orig.Amount)
	}
}

func TestOrderCreatedPayload_JSONKeys(t *testing.T) {
	p := OrderCreatedPayload{OrderID: "ORD-001", Amount: 500000}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	want := `{"orderId":"ORD-001","amount":500000}`
	if string(data) != want {
		t.Errorf("JSON = %s, want %s", string(data), want)
	}
}

func TestEventTypeOrderCreatedConstant(t *testing.T) {
	if EventTypeOrderCreated != "OrderCreated" {
		t.Errorf("EventTypeOrderCreated = %q, want OrderCreated", EventTypeOrderCreated)
	}
}

func TestOrderCreatedPayload_Validate(t *testing.T) {
	tests := []struct {
		name    string
		payload OrderCreatedPayload
		wantErr bool
	}{
		{"valid", OrderCreatedPayload{OrderID: "ORD-001", Amount: 500000}, false},
		{"empty OrderID", OrderCreatedPayload{OrderID: "", Amount: 500000}, true},
		{"zero amount", OrderCreatedPayload{OrderID: "ORD-001", Amount: 0}, true},
		{"negative amount", OrderCreatedPayload{OrderID: "ORD-001", Amount: -1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.payload.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
