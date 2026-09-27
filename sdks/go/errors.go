package beamnetworksdk

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// ProviderTransferError reports a non-recoverable provider transfer failure
// after the SDK cancelled the transfer and aborted the multipart uploads it
// created. Errors holds the original failure followed by any cancellation and
// cleanup failures; errors.Is and errors.As search all of them.
type ProviderTransferError struct {
	TransferID               string
	TransferCancelled        bool
	MultipartCleanupComplete bool
	Cause                    error
	Errors                   []error
}

func newProviderTransferError(transferID string, cause error, cancelErr error, cleanupErr error) *ProviderTransferError {
	errs := []error{cause}
	if cancelErr != nil {
		errs = append(errs, cancelErr)
	}
	if cleanupErr != nil {
		errs = append(errs, cleanupErr)
	}
	return &ProviderTransferError{
		TransferID:               transferID,
		TransferCancelled:        cancelErr == nil,
		MultipartCleanupComplete: cleanupErr == nil,
		Cause:                    cause,
		Errors:                   errs,
	}
}

// Error never includes provider messages, which may carry signed URLs.
func (err *ProviderTransferError) Error() string {
	return fmt.Sprintf(
		"provider transfer failed for %s (transfer_cancelled=%t, multipart_cleanup_complete=%t)",
		err.TransferID, err.TransferCancelled, err.MultipartCleanupComplete,
	)
}

func (err *ProviderTransferError) Unwrap() []error {
	return err.Errors
}

var safeErrorCodePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// safeErrorCode describes an error by code and status only, never by message.
func safeErrorCode(err error) string {
	if err == nil {
		return "UnknownError"
	}
	code := errorName(err)
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && safeErrorCodePattern.MatchString(apiErr.ErrorCode()) {
		code = apiErr.ErrorCode()
	}
	status := 0
	var responseErr *smithyhttp.ResponseError
	var lifecycleErr *LifecycleRequestError
	if errors.As(err, &responseErr) {
		status = responseErr.HTTPStatusCode()
	} else if errors.As(err, &lifecycleErr) {
		status = lifecycleErr.Status
	}
	if status >= 100 && status <= 599 {
		return fmt.Sprintf("%s:status=%d", code, status)
	}
	return code
}

// errorName is a stable, message-free name for an error's type.
func errorName(err error) string {
	switch {
	case err == nil:
		return "UnknownError"
	case errors.Is(err, context.Canceled):
		return "Canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "DeadlineExceeded"
	}
	var lifecycleErr *LifecycleRequestError
	if errors.As(err, &lifecycleErr) {
		return "LifecycleRequestError"
	}
	errorType := reflect.TypeOf(err)
	for errorType.Kind() == reflect.Pointer {
		errorType = errorType.Elem()
	}
	name := errorType.Name()
	if first, _ := utf8.DecodeRuneInString(name); name == "" || !unicode.IsUpper(first) {
		return "Error"
	}
	return name
}

var sensitiveErrorPattern = regexp.MustCompile(`(?i)https?://|x-amz|password|secret|token|authorization|credential`)

// integrityAuditErrorSummary is a URL- and credential-safe summary of an
// integrity grant submission failure, capped at 200 characters.
func integrityAuditErrorSummary(err error) string {
	if err == nil {
		return "unknown_error"
	}
	message := strings.TrimSpace(err.Error())
	if message == "" || sensitiveErrorPattern.MatchString(message) {
		return errorName(err)
	}
	if utf8.RuneCountInString(message) > 200 {
		message = string([]rune(message)[:200])
	}
	return message
}

func trimmedEmpty(value string) bool {
	return strings.TrimSpace(value) == ""
}
