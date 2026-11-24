package event

import (
	"errors"
	"fmt"
	"strings"
)

// Conversation key format (decision 0006): the conversation of a company and
// a customer is keyed by
//
//	<company ID>|<customer ID>
//
// with "%" written as "%25" and "|" written as "%7C" inside each ID. After
// this escaping neither ID contains a raw "|", so the key has exactly one
// separator and two different pairs never get the same key. For normal IDs
// (a Bluesky DID has neither character) the key is the two IDs as they are,
// which keeps it readable in tools.
//
// The key is used as the Kafka record key in the events and conversations
// topics, so the default partitioner puts every record of one conversation
// in one partition. ParseConversationKey gives the two IDs back.
//
// The key holds no channel, as decision 0006 defines it.

const (
	keySeparator = "|"
)

var (
	keyEscaper   = strings.NewReplacer("%", "%25", "|", "%7C")
	keyUnescaper = strings.NewReplacer("%25", "%", "%7C", "|")
)

// ConversationKey returns the record key of the conversation of a company
// and a customer. Both IDs should be non-empty. The function does not check
// this, because the events that carry the IDs are validated on their own.
func ConversationKey(companyID, customerID string) []byte {
	return []byte(keyEscaper.Replace(companyID) + keySeparator + keyEscaper.Replace(customerID))
}

// ParseConversationKey returns the company ID and the customer ID of a key
// made by ConversationKey. It fails when the key does not have exactly one
// separator, when either part is empty, or when a part has an escape
// sequence that ConversationKey does not write.
func ParseConversationKey(key []byte) (companyID, customerID string, err error) {
	company, customer, ok := strings.Cut(string(key), keySeparator)
	if !ok {
		return "", "", errors.New("parse conversation key: no separator")
	}
	if strings.Contains(customer, keySeparator) {
		return "", "", errors.New("parse conversation key: more than one separator")
	}
	companyID, err = unescapeKeyPart(company)
	if err != nil {
		return "", "", fmt.Errorf("parse conversation key: company ID: %w", err)
	}
	customerID, err = unescapeKeyPart(customer)
	if err != nil {
		return "", "", fmt.Errorf("parse conversation key: customer ID: %w", err)
	}
	return companyID, customerID, nil
}

// unescapeKeyPart reverses the escaping of one key part. Every "%" must
// start "%25" or "%7C", so each key maps back to exactly one pair.
func unescapeKeyPart(s string) (string, error) {
	if s == "" {
		return "", errors.New("empty")
	}
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			continue
		}
		if !strings.HasPrefix(s[i:], "%25") && !strings.HasPrefix(s[i:], "%7C") {
			return "", errors.New("invalid escape sequence")
		}
		i += 2
	}
	return keyUnescaper.Replace(s), nil
}

// CompanyKey returns the record key of a team event in the routing topic:
// the company ID as it is (decision 0009).
func CompanyKey(companyID string) []byte {
	return []byte(companyID)
}

// AuthorKey returns the record key of a control event in the events.control
// topic: the author ID as it is (decision 0006).
func AuthorKey(authorID string) []byte {
	return []byte(authorID)
}
