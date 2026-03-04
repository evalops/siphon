package providers

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stripe/stripe-go/v82"
)

func TestDeriveEntityAction(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		hint      string
		entity    string
		action    string
	}{
		{name: "dot separated", eventType: "invoice.paid", entity: "invoice", action: "paid"},
		{name: "single token", eventType: "push", entity: "push", action: "push"},
		{name: "empty type", eventType: "", entity: "event", action: "updated"},
		{name: "action hint wins", eventType: "contact.updated", hint: "deleted", entity: "contact", action: "deleted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entity, action := deriveEntityAction(tt.eventType, tt.hint)
			if entity != tt.entity || action != tt.action {
				t.Fatalf("deriveEntityAction(%q,%q) = (%q,%q), want (%q,%q)", tt.eventType, tt.hint, entity, action, tt.entity, tt.action)
			}
		})
	}
}

func TestNormalizeGeneric(t *testing.T) {
	payload := []byte(`{"id":"deal_123","timestamp":"2026-03-03T14:22:00Z","amount":85}`)
	evt, err := NormalizeGeneric("acme", "deal.updated", "", "evt_1", "tenant-1", payload)
	if err != nil {
		t.Fatalf("NormalizeGeneric returned error: %v", err)
	}

	if evt.Provider != "acme" || evt.EntityType != "deal" || evt.Action != "updated" {
		t.Fatalf("unexpected normalized event: %+v", evt)
	}
	if evt.EntityID != "deal_123" {
		t.Fatalf("expected entity id deal_123, got %q", evt.EntityID)
	}
	if evt.ProviderEventID != "evt_1" || evt.TenantID != "tenant-1" {
		t.Fatalf("unexpected ids: %+v", evt)
	}
	if evt.ProviderTime.UTC().Format(time.RFC3339) != "2026-03-03T14:22:00Z" {
		t.Fatalf("unexpected provider time: %s", evt.ProviderTime)
	}
}

func TestNormalizeGitHubPullRequestAndPush(t *testing.T) {
	prPayload := []byte(`{"action":"closed","pull_request":{"id":99}}`)
	prEvt, err := NormalizeGitHub("pull_request", "delivery-1", "tenant-gh", prPayload)
	if err != nil {
		t.Fatalf("NormalizeGitHub (pull_request) error: %v", err)
	}
	if prEvt.EntityType != "pull_request" || prEvt.Action != "closed" || prEvt.EntityID != "99" {
		t.Fatalf("unexpected PR normalized event: %+v", prEvt)
	}

	pushEvt, err := NormalizeGitHub("push", "delivery-2", "tenant-gh", []byte(`{"head_commit":{"id":"abc"}}`))
	if err != nil {
		t.Fatalf("NormalizeGitHub (push) error: %v", err)
	}
	if pushEvt.Action != "created" {
		t.Fatalf("expected push action created, got %q", pushEvt.Action)
	}
}

func TestNormalizeHubSpot(t *testing.T) {
	payload := []byte(`[{"subscriptionType":"deal.propertyChange","objectType":"deal","objectId":12345,"propertyName":"dealstage","propertyValue":"closedwon","occurredAt":"2026-03-03T14:22:00Z"}]`)
	evt, err := NormalizeHubSpot("evt_hs_1", "tenant-hs", payload)
	if err != nil {
		t.Fatalf("NormalizeHubSpot error: %v", err)
	}
	if evt.EntityType != "deal" || evt.Action != "updated" || evt.EntityID != "12345" {
		t.Fatalf("unexpected hubspot event: %+v", evt)
	}
	chg, ok := evt.Changes["dealstage"]
	if !ok || chg.To != "closedwon" {
		t.Fatalf("expected dealstage change, got %+v", evt.Changes)
	}
	if evt.ProviderTime.UTC().Format(time.RFC3339) != "2026-03-03T14:22:00Z" {
		t.Fatalf("unexpected hubspot provider time: %s", evt.ProviderTime)
	}

	creationPayload := []byte(`{"subscriptionType":"contact.creation","objectType":"contact","objectId":"c_1"}`)
	createdEvt, err := NormalizeHubSpot("evt_hs_2", "tenant-hs", creationPayload)
	if err != nil {
		t.Fatalf("NormalizeHubSpot creation error: %v", err)
	}
	if createdEvt.Action != "created" || createdEvt.EntityType != "contact" || createdEvt.EntityID != "c_1" {
		t.Fatalf("unexpected created event: %+v", createdEvt)
	}
}

func TestNormalizeLinear(t *testing.T) {
	payload := []byte(`{"type":"Issue","data":{"id":"ISS-1"},"webhookTimestamp":"2026-03-03T14:22:00Z"}`)
	evt, err := NormalizeLinear("issue.updated", "lin_del_1", "tenant-lin", payload)
	if err != nil {
		t.Fatalf("NormalizeLinear error: %v", err)
	}

	if evt.EntityType != "issue" || evt.Action != "updated" {
		t.Fatalf("unexpected linear entity/action: %+v", evt)
	}
	if evt.EntityID != "ISS-1" {
		t.Fatalf("unexpected linear entity id: %q", evt.EntityID)
	}
	if evt.ProviderTime.UTC().Format(time.RFC3339) != "2026-03-03T14:22:00Z" {
		t.Fatalf("unexpected linear provider time: %s", evt.ProviderTime)
	}
}

func TestNormalizeShopify(t *testing.T) {
	payload := []byte(`{"id":123}`)
	evt, err := NormalizeShopify("orders/paid", "shop_evt_1", "tenant-shop", payload)
	if err != nil {
		t.Fatalf("NormalizeShopify error: %v", err)
	}

	if evt.EntityType != "orders" || evt.Action != "paid" || evt.EntityID != "123" {
		t.Fatalf("unexpected shopify normalized event: %+v", evt)
	}

	fallbackPayload := []byte(`{"admin_graphql_api_id":"gid://shopify/Order/44"}`)
	fallbackEvt, err := NormalizeShopify("orders/updated", "shop_evt_2", "tenant-shop", fallbackPayload)
	if err != nil {
		t.Fatalf("NormalizeShopify fallback error: %v", err)
	}
	if fallbackEvt.EntityID != "gid://shopify/Order/44" {
		t.Fatalf("expected graphql id fallback, got %q", fallbackEvt.EntityID)
	}
}

func TestNormalizeStripe(t *testing.T) {
	evt := stripe.Event{
		ID:      "evt_1",
		Type:    stripe.EventType("invoice.paid"),
		Created: 1709562120,
		Data: &stripe.EventData{
			Raw: json.RawMessage(`{"id":"in_123","amount_paid":500}`),
		},
	}
	norm, err := NormalizeStripe(evt, "tenant-stripe")
	if err != nil {
		t.Fatalf("NormalizeStripe error: %v", err)
	}

	if norm.Provider != "stripe" || norm.EntityType != "invoice" || norm.Action != "paid" {
		t.Fatalf("unexpected stripe normalized event: %+v", norm)
	}
	if norm.EntityID != "in_123" {
		t.Fatalf("unexpected stripe entity id: %q", norm.EntityID)
	}
	if norm.ProviderTime.UTC() != time.Unix(1709562120, 0).UTC() {
		t.Fatalf("unexpected stripe provider time: %s", norm.ProviderTime)
	}
}

func TestNormalizeStripeHandlesNilData(t *testing.T) {
	evt := stripe.Event{
		ID:   "evt_2",
		Type: stripe.EventType("customer.updated"),
	}
	norm, err := NormalizeStripe(evt, "tenant-stripe")
	if err != nil {
		t.Fatalf("NormalizeStripe nil-data error: %v", err)
	}
	if norm.EntityID != "unknown" {
		t.Fatalf("expected unknown entity id when data is nil, got %q", norm.EntityID)
	}
}
