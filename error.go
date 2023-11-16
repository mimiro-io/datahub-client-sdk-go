package datahubclient

import "fmt"

// RequestError is an error that occurs when there is an issue making the request
// or with the request data.
// Check the inner error for more details.
type RequestError struct {
	Err error
	Msg string
}

func (e *RequestError) Error() string {
	return fmt.Sprintf("%s: %v", e.Msg, e.Err)
}

func (e *RequestError) Unwrap() error {
	return e.Err
}

// AuthenticationError is an error that occurs when there is an issue
// authenticating with the server.
// Check the inner error for more details.
type AuthenticationError struct {
	Err error
	Msg string
}

func (e *AuthenticationError) Error() string {
	return fmt.Sprintf("%s: %v", e.Msg, e.Err)
}

func (e *AuthenticationError) Unwrap() error {
	return e.Err
}

// ClientProcessingError is an error that occurs when there is an issue
// processing the response from the server.
// Check the inner error for more details.
type ClientProcessingError struct {
	Err error
	Msg string
}

func (e *ClientProcessingError) Error() string {
	return fmt.Sprintf("%s: %v", e.Msg, e.Err)
}

func (e *ClientProcessingError) Unwrap() error {
	return e.Err
}

// ParameterError is an error that occurs when there is an issue
// with the parameters passed to the client function.
// Check the inner error for more details.
type ParameterError struct {
	Err error
	Msg string
}

func (e *ParameterError) Error() string {
	return fmt.Sprintf("%s: %v", e.Msg, e.Err)
}

func (e *ParameterError) Unwrap() error {
	return e.Err
}
