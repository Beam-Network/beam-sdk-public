package beamnetworksdk

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func lifecycleRefusal(t *testing.T, status int, replyError map[string]any) error {
	t.Helper()
	control := controlWithCachedToken("test-token")
	requests := 0
	control.requestOverride = func(context.Context, string, []byte) ([]byte, error) {
		requests++
		return marshalMsgpack(map[string]any{
			"ok": false, "status": status, "runtime_epoch": "runtime-one", "transport_epoch": "transport-one",
			"error": replyError,
		})
	}
	err := control.request(context.Background(), "transfer.create", map[string]any{}, "transfer-one", nil, "create")
	if err == nil {
		t.Fatal("the refusal must fail the request")
	}
	if requests != 1 {
		t.Fatalf("refusals are not retried, requests=%d", requests)
	}
	return err
}

func TestInsufficientCreditsErrorCarriesTheAmounts(t *testing.T) {
	err := lifecycleRefusal(t, 402, map[string]any{
		"code":    "insufficient_credits",
		"message": "transfer needs an estimated 3.5 credits; the organization has 1.25",
		"detail":  map[string]any{"credits_required": 3.5, "credits_available": 1.25, "reason": "key_budget_blocked"},
	})
	var credits *InsufficientCreditsError
	if !errors.As(err, &credits) {
		t.Fatalf("error = %#v", err)
	}
	if credits.Status != 402 || credits.Code != "insufficient_credits" || credits.Message != "transfer needs an estimated 3.5 credits; the organization has 1.25" {
		t.Fatalf("refusal = %#v", credits)
	}
	if credits.CreditsRequired == nil || *credits.CreditsRequired != 3.5 || credits.CreditsAvailable == nil || *credits.CreditsAvailable != 1.25 || credits.Reason != "key_budget_blocked" {
		t.Fatalf("amounts = %v %v %q", credits.CreditsRequired, credits.CreditsAvailable, credits.Reason)
	}
	var lifecycleErr *LifecycleRequestError
	if !errors.As(err, &lifecycleErr) || lifecycleErr.Status != 402 {
		t.Fatalf("the refusal must remain a lifecycle error: %#v", err)
	}

	whole := lifecycleRefusal(t, 402, map[string]any{
		"code":    "insufficient_credits",
		"message": "transfer needs an estimated 2 credits; the organization has 0",
		"detail":  map[string]any{"credits_required": 2, "credits_available": 0},
	})
	if !errors.As(whole, &credits) || credits.CreditsRequired == nil || *credits.CreditsRequired != 2 || credits.CreditsAvailable == nil || *credits.CreditsAvailable != 0 || credits.Reason != "" {
		t.Fatalf("whole amounts = %#v", whole)
	}

	unknown := lifecycleRefusal(t, 402, map[string]any{
		"code":    "insufficient_credits",
		"message": "organization credit balance is exhausted",
		"detail":  map[string]any{"credits_required": nil, "credits_available": nil},
	})
	if !errors.As(unknown, &credits) || credits.CreditsRequired != nil || credits.CreditsAvailable != nil {
		t.Fatalf("unknown amounts = %#v", unknown)
	}
}

func TestBillingKeyAndPermissionRefusalsAreTyped(t *testing.T) {
	var unlinked *BillingAccountNotLinkedError
	if err := lifecycleRefusal(t, 402, map[string]any{"code": "billing_account_not_linked", "message": "this API key is not linked to a billing account"}); !errors.As(err, &unlinked) || unlinked.Status != 402 {
		t.Fatalf("unlinked = %#v", err)
	}
	for _, status := range []int{401, 403} {
		var invalid *APIKeyInvalidError
		if err := lifecycleRefusal(t, status, map[string]any{"code": "api_key_invalid", "message": "API key is expired"}); !errors.As(err, &invalid) || invalid.Status != status || invalid.Message != "API key is expired" {
			t.Fatalf("invalid key (%d) = %#v", status, err)
		}
	}
	var permission *PermissionRequiredError
	if err := lifecycleRefusal(t, 403, map[string]any{"code": "permission_required", "message": "API key lacks transfers:create", "detail": map[string]any{"permission": "transfers:create"}}); !errors.As(err, &permission) || permission.Permission != "transfers:create" {
		t.Fatalf("permission = %#v", err)
	}
	other := lifecycleRefusal(t, 409, map[string]any{"code": "route_generation_mismatch", "message": "stale generation"})
	if _, ok := other.(*LifecycleRequestError); !ok {
		t.Fatalf("other codes stay *LifecycleRequestError: %#v", other)
	}
}

func TestCreditsLowWarningsReachTheHandlerOncePerMessage(t *testing.T) {
	received := make(chan Warning, 8)
	client, err := New(WithAPIKey("b1m_test"), WithNATSURL("nats://127.0.0.1:4222"), WithWarningHandler(func(warning Warning) {
		received <- warning
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	control := client.control.(*natsControl)
	control.authToken = "test-token"
	control.authExpiresAt = time.Now().Unix() + 3600
	remaining := []float64{4.5, 4.5, 3}
	calls := 0
	control.requestOverride = func(context.Context, string, []byte) ([]byte, error) {
		value := remaining[calls]
		calls++
		return marshalMsgpack(map[string]any{
			"ok": true, "status": 200, "runtime_epoch": "runtime-one", "transport_epoch": "transport-one",
			"payload": map[string]any{"transfer_id": "transfer-one", "status": "in_progress"},
			"warnings": []any{map[string]any{
				"code":    "credits_low",
				"message": "Credit balance is low: " + strconv.FormatFloat(value, 'g', -1, 64) + " credits remaining.",
				"detail":  map[string]any{"remaining": value, "low_threshold": 5},
			}},
		})
	}
	for range remaining {
		if err := control.request(context.Background(), "transfer.status", map[string]any{}, "transfer-one", nil); err != nil {
			t.Fatal(err)
		}
	}
	messages := map[string]bool{}
	for len(messages) < 2 {
		select {
		case warning := <-received:
			if warning.Code != "credits_low" || warning.Detail["low_threshold"] == nil {
				t.Fatalf("warning = %#v", warning)
			}
			messages[warning.Message] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("warnings = %v", messages)
		}
	}
	if !messages["Credit balance is low: 4.5 credits remaining."] || !messages["Credit balance is low: 3 credits remaining."] {
		t.Fatalf("warnings = %v", messages)
	}
	select {
	case warning := <-received:
		t.Fatalf("a repeated warning must not be reported again: %#v", warning)
	case <-time.After(50 * time.Millisecond):
	}
}
