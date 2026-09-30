package beamnetworksdk

import (
	"context"
	"time"
)

// ProviderTransferOptions configures a provider transfer. It mirrors the
// TypeScript ProviderTransferCreateInput.
//
// Callbacks receive the foreground ctx and may be called while the SDK holds
// the transfer's route-stream lock, so they must not call back into this
// transfer's streaming.
type ProviderTransferOptions struct {
	// Sources and Destinations are the storage to read from and write to. Their
	// credentials must not be restricted to specific IP addresses or networks
	// (for example Cloudflare R2 API-token client IP filtering, S3 bucket
	// policies with aws:SourceIp, or VPC-only endpoints). Beam moves data
	// through many workers on different networks, so restricted credentials
	// make the transfer fail.
	Sources      []ProviderSource
	Destinations []ProviderDestination
	Name         string
	// ExpiresIn is the lifetime of signed URLs. Defaults to one hour.
	ExpiresIn time.Duration
	// Distribute defaults to true; set it to Bool(false) to prepare and stream
	// signed routes without distributing the transfer.
	Distribute *bool
	// ChunkSize requests a plan chunk size. BeamCore may raise it; the response
	// carries the effective value. A Hugging Face destination dictates its own.
	ChunkSize int64
	// IdempotencyKey is a stable caller identity used to derive the transfer id
	// and lifecycle request ids.
	IdempotencyKey string
	// RouteGenerationID is internal recovery state; callers normally omit it.
	RouteGenerationID string

	// Ownership is an ownership fence, separate from the foreground ctx. When it
	// is done, the SDK stops signing, route replay, multipart creation, and
	// route recovery signing for this transfer and releases its recovery lease.
	// It does not cancel the transfer or abort multipart uploads, which a
	// replacement owner may be using. Operations report context.Cause(Ownership).
	Ownership context.Context

	// OnBeforeTransferPrepare runs after sources are signed and before
	// transfer.prepare. An error aborts before the transfer exists.
	OnBeforeTransferPrepare func(ctx context.Context) error
	// OnPrepared runs after transfer.prepare succeeds and before routes stream,
	// including during resume. An error returns before streaming while the
	// recovery lease keeps the prepared transfer recoverable in the background.
	OnPrepared func(ctx context.Context, prepared *TransferPrepareResponse) error
	// OnMultipartGroupReady runs after the SDK creates a multipart upload and
	// before that group's routes stream. The identity never carries credentials,
	// headers, or signed URLs. An error aborts the new upload and fails the
	// transfer closed. It may be called concurrently for different groups.
	OnMultipartGroupReady func(ctx context.Context, group ProviderMultipartGroupIdentity) error
	// ThrowIfCancelled is polled before preparing and before each planned chunk
	// of the initial stream. An error stops streaming while the recovery lease
	// keeps the prepared transfer recoverable in the background.
	ThrowIfCancelled func(ctx context.Context, transferID string) error
}

func (options ProviderTransferOptions) input() providerTransferInput {
	return providerTransferInput{
		sources:                 options.Sources,
		destinations:            options.Destinations,
		name:                    options.Name,
		expiresIn:               options.ExpiresIn,
		distribute:              options.Distribute == nil || *options.Distribute,
		chunkSize:               options.ChunkSize,
		idempotencyKey:          options.IdempotencyKey,
		routeGenerationID:       options.RouteGenerationID,
		ownership:               options.Ownership,
		onBeforeTransferPrepare: options.OnBeforeTransferPrepare,
		onPrepared:              options.OnPrepared,
		onMultipartGroupReady:   options.OnMultipartGroupReady,
		throwIfCancelled:        options.ThrowIfCancelled,
	}
}

// PrepareProviderTransferWithOptions signs provider sources and destinations,
// prepares the transfer, and streams its signed routes. It is the options
// form of PrepareProviderTransfer and the TypeScript prepareProviderTransfer.
//
// Source and destination credentials must not be restricted to specific IP
// addresses or networks (for example Cloudflare R2 API-token client IP
// filtering, S3 bucket policies with aws:SourceIp, or VPC-only endpoints). Beam
// moves data through many workers on different networks, so restricted
// credentials make the transfer fail.
func (client *Client) PrepareProviderTransferWithOptions(ctx context.Context, options ProviderTransferOptions) (*TransferPrepareResponse, error) {
	return client.executeProviderTransfer(ctx, options.input(), nil)
}

// ProviderTransferResumeOptions re-attaches a provider transfer after a
// process restart. IdempotencyKey and RouteGenerationID are ignored.
type ProviderTransferResumeOptions struct {
	ProviderTransferOptions
	// TransferID is the prepared transfer to resume.
	TransferID string
	// MultipartGroups are the complete identities recorded from
	// OnMultipartGroupReady. Resume never creates replacement uploads, so every
	// multipart group of the plan must be present, unchanged, and unique.
	MultipartGroups []ProviderMultipartGroupIdentity
}

// ResumeProviderTransfer re-prepares an existing transfer by explicit id with a
// fresh request and route generation, validates the saved multipart identities
// against the plan, and streams routes that reuse those upload ids. It then
// serves route recovery signing from the current provider configs. OnPrepared
// runs before routes are streamed.
//
// Source and destination credentials must not be restricted to specific IP
// addresses or networks (for example Cloudflare R2 API-token client IP
// filtering, S3 bucket policies with aws:SourceIp, or VPC-only endpoints). Beam
// moves data through many workers on different networks, so restricted
// credentials make the transfer fail.
func (client *Client) ResumeProviderTransfer(ctx context.Context, options ProviderTransferResumeOptions) (*TransferPrepareResponse, error) {
	if err := validateID(options.TransferID, "transferID"); err != nil {
		return nil, err
	}
	input := options.ProviderTransferOptions.input()
	input.idempotencyKey = ""
	input.routeGenerationID = ""
	return client.executeProviderTransfer(ctx, input, &providerResume{transferID: options.TransferID, multipartGroups: options.MultipartGroups})
}

// CreateProviderTransfer is the TypeScript createTransfer: a provider transfer
// that distributes by default. The raw CreateTransfer keeps its Go name.
//
// Source and destination credentials must not be restricted to specific IP
// addresses or networks (for example Cloudflare R2 API-token client IP
// filtering, S3 bucket policies with aws:SourceIp, or VPC-only endpoints). Beam
// moves data through many workers on different networks, so restricted
// credentials make the transfer fail.
func (client *Client) CreateProviderTransfer(ctx context.Context, options ProviderTransferOptions) (*TransferPrepareResponse, error) {
	return client.PrepareProviderTransferWithOptions(ctx, options)
}
