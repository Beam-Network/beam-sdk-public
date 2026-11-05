package beamnetworksdk

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestBillingParentIsSentOnCreateAndPrepare(t *testing.T) {
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake)
	ctx := context.Background()
	const parent = "workflow.run:org_1/run-42@a+b=c"
	create := TransferCreateRequest{
		Sources:      []SourceConfig{{"type": "http", "url": "https://source.example/file.bin"}},
		Destinations: []DestConfig{{"type": "http", "url": "https://dest.example/file.bin"}},
		TotalSize:    1024,
	}
	prepare := TransferPrepareRequest{
		Sources:      []PreparedHTTPSource{{SourceID: "src_0", Type: "http", URL: "https://source.example/file.bin", Size: 4096}},
		Destinations: []PreparedDestination{{DestinationID: "dst_0", Provider: "http", Mode: "http_chunks"}},
	}
	withParent := create
	withParent.BillingParent = parent
	if _, err := client.CreateTransfer(ctx, withParent); err != nil {
		t.Fatal(err)
	}
	preparedWithParent := prepare
	preparedWithParent.BillingParent = parent
	if _, err := client.PrepareTransferWithRequest(ctx, preparedWithParent); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateTransfer(ctx, create); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PrepareTransferWithRequest(ctx, prepare); err != nil {
		t.Fatal(err)
	}
	if len(fake.calls) != 4 {
		t.Fatalf("calls = %d", len(fake.calls))
	}
	for index, want := range []string{parent, parent, "", ""} {
		got, sent := fake.calls[index].payload["billing_parent"]
		if want == "" && sent {
			t.Fatalf("call %d must omit billing_parent: %+v", index, fake.calls[index].payload)
		}
		if want != "" && got != want {
			t.Fatalf("call %d billing_parent = %v, want %s", index, got, want)
		}
	}

	longest := create
	longest.BillingParent = strings.Repeat("a", 256)
	if _, err := client.CreateTransfer(ctx, longest); err != nil {
		t.Fatal(err)
	}
	storage := newFakeS3(t)
	for _, invalid := range []string{strings.Repeat("a", 257), "workflow run", "run#1", "résumé"} {
		before := len(fake.calls)
		invalidCreate := create
		invalidCreate.BillingParent = invalid
		if _, err := client.CreateTransfer(ctx, invalidCreate); err == nil || !strings.Contains(err.Error(), "billingParent must be 1 to 256") {
			t.Fatalf("create with %q: %v", invalid, err)
		}
		invalidPrepare := prepare
		invalidPrepare.BillingParent = invalid
		if _, err := client.PrepareTransferWithRequest(ctx, invalidPrepare); err == nil || !strings.Contains(err.Error(), "billingParent must be 1 to 256") {
			t.Fatalf("prepare with %q: %v", invalid, err)
		}
		if _, err := client.CreateProviderTransfer(ctx, ProviderTransferOptions{
			Sources:       []ProviderSource{r2Source(storage.URL)},
			Destinations:  []ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
			BillingParent: invalid,
		}); err == nil || !strings.Contains(err.Error(), "billingParent must be 1 to 256") {
			t.Fatalf("provider transfer with %q: %v", invalid, err)
		}
		if len(fake.calls) != before {
			t.Fatalf("an invalid billing parent must fail before any request: %+v", fake.calls[before:])
		}
	}
	if heads, creates, _ := storage.counts(); heads != 0 || creates != 0 {
		t.Fatalf("an invalid billing parent must fail before touching storage: heads=%d creates=%d", heads, creates)
	}
}

func TestProviderTransfersSendTheBillingParent(t *testing.T) {
	storage := newFakeS3(t)
	fake := &fakeTransferControl{}
	client := newTestClient(t, fake, WithRouteSigningConcurrency(4))
	ctx := context.Background()
	if _, err := client.CreateProviderTransfer(ctx, ProviderTransferOptions{
		Sources:       []ProviderSource{r2Source(storage.URL)},
		Destinations:  []ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
		Distribute:    Bool(false),
		BillingParent: "workflow.run:run_1",
	}); err != nil {
		t.Fatal(err)
	}
	transferID := "77777777-7777-4777-8777-777777777777"
	groups := []ProviderMultipartGroupIdentity{{
		TransferID: transferID, MultipartGroupID: transferID + ":dst_0:src_0:out/file.bin", SourceID: "src_0", DestinationID: "dst_0",
		ObjectKey: "out/file.bin", UploadID: "upload-existing", ExpectedObjectSize: 1024, ExpectedPartCount: 1,
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano),
	}}
	if _, err := client.ResumeProviderTransfer(ctx, ProviderTransferResumeOptions{
		ProviderTransferOptions: ProviderTransferOptions{
			Sources:       []ProviderSource{r2Source(storage.URL)},
			Destinations:  []ProviderDestination{r2Destination(storage.URL, "out/file.bin")},
			BillingParent: "workflow.run:run_2",
		},
		TransferID:      transferID,
		MultipartGroups: groups,
	}); err != nil {
		t.Fatal(err)
	}
	prepares := fake.callsOf("transfer.prepare")
	if len(prepares) != 2 || prepares[0].payload["billing_parent"] != "workflow.run:run_1" || prepares[1].payload["billing_parent"] != "workflow.run:run_2" {
		t.Fatalf("provider prepares must carry their billing parents: %+v", prepares)
	}
}
