package gwapi

import (
	"errors"
	"fmt"
)

// ErrUnknownType is returned, wrapped, when a message has a type this
// version does not know. The server closes the connection with
// CloseBadMessage. A client skips such a server message, so a newer server
// can add types.
var ErrUnknownType = errors.New("unknown message type")

// ErrUnsupportedVersion is returned, wrapped, by DecodeClient for a Hello
// whose version is not ProtocolVersion. The server closes the connection
// with CloseUnsupportedVersion.
var ErrUnsupportedVersion = errors.New("unsupported protocol version")

// fieldErrors collects one error per invalid field, named by its JSON
// name, as in the event package. Errors never contain post text or a token.
type fieldErrors []error

func (e *fieldErrors) missing(field string) {
	*e = append(*e, fmt.Errorf("%s is not set", field))
}

func (e *fieldErrors) invalid(field, format string, args ...any) {
	*e = append(*e, fmt.Errorf("%s: "+format, append([]any{field}, args...)...))
}

// within adds a prefix to every error added by fn, for nested objects and
// list items.
func (e *fieldErrors) within(prefix string, fn func(errs *fieldErrors)) {
	var inner fieldErrors
	fn(&inner)
	for _, err := range inner {
		*e = append(*e, fmt.Errorf("%s.%w", prefix, err))
	}
}

func (e fieldErrors) join() error {
	return errors.Join(e...)
}
