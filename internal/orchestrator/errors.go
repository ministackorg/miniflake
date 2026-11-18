package orchestrator

import "fmt"

// SnowflakeError is an error whose Snowflake error code, SQLSTATE and message
// are already known (USE of a missing object, unsupported routine language,
// ...). The server sends these fields to the driver unchanged instead of
// mapping the error text.
type SnowflakeError struct {
	Code     string
	SQLState string
	Message  string
}

func (e *SnowflakeError) Error() string { return e.Message }

// errUnsupported is Snowflake's "Unsupported feature" error (000002 / 0A000).
func errUnsupported(feature string) error {
	return &SnowflakeError{
		Code:     "000002",
		SQLState: "0A000",
		Message:  fmt.Sprintf("Unsupported feature '%s'.", feature),
	}
}
