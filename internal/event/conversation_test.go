package event

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

func validConversation(typ ConversationType) Conversation {
	c := Conversation{
		Type:             typ,
		Channel:          ChannelBluesky,
		CompanyID:        testCompany,
		CustomerID:       testCustomer,
		Session:          1,
		Seq:              1,
		SourceTime:       testTime,
		OpenedAt:         testTime,
		Lang:             "en",
		CustomerMessages: 1,
		MessageID:        testMsgID,
		Direction:        FromCustomer,
	}
	switch typ {
	case TypeMessageAdded:
		c.Seq = 2
		c.CustomerMessages = 2
	case TypeAnswered:
		c.Seq = 3
		c.CompanyMessages = 1
		c.Direction = FromCompany
		c.MessageID = "at://did:plc:testalice0000000000000000/app.bsky.feed.post/ccc333"
		c.ResponseTime = 90*time.Second + 250*time.Microsecond
	case TypeClosed:
		c.Seq = 4
		c.MessageID = ""
		c.Direction = ""
		c.CloseReason = CloseInactivity
	}
	return c
}

func TestConversationRoundTrip(t *testing.T) {
	reopened := validConversation(TypeOpened)
	reopened.Session, reopened.Seq = 2, 9
	zeroResponse := validConversation(TypeAnswered)
	zeroResponse.ResponseTime = 0
	companyAdded := validConversation(TypeMessageAdded)
	companyAdded.Direction = FromCompany

	tests := []struct {
		name string
		in   Conversation
	}{
		{name: "opened", in: validConversation(TypeOpened)},
		{name: "message added", in: validConversation(TypeMessageAdded)},
		{name: "message added by company", in: companyAdded},
		{name: "answered", in: validConversation(TypeAnswered)},
		{name: "answered at once", in: zeroResponse},
		{name: "closed", in: validConversation(TypeClosed)},
		{name: "reopened", in: reopened},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := MarshalConversation(tt.in)
			if err != nil {
				t.Fatalf("MarshalConversation() error = %v", err)
			}
			if bytes.Contains(b, []byte(testText)) {
				t.Errorf("encoded event %s contains post text", b)
			}
			out, err := UnmarshalConversation(b)
			if err != nil {
				t.Fatalf("UnmarshalConversation() error = %v", err)
			}
			if !reflect.DeepEqual(out, tt.in) {
				t.Errorf("round trip:\n got %+v\nwant %+v", out, tt.in)
			}
		})
	}
}

func TestConversationReopenedAndKey(t *testing.T) {
	c := validConversation(TypeOpened)
	if c.Reopened() {
		t.Error("Reopened() = true for session 1")
	}
	c.Session = 2
	if !c.Reopened() {
		t.Error("Reopened() = false for session 2")
	}
	if !bytes.Equal(c.Key(), ConversationKey(testCompany, testCustomer)) {
		t.Errorf("Key() = %q", c.Key())
	}
}

func TestMarshalConversationFormat(t *testing.T) {
	tests := []struct {
		name string
		in   Conversation
		want string
	}{
		{
			name: "answered",
			in:   validConversation(TypeAnswered),
			want: `{"type":"answered","channel":"bluesky","company_id":"` + testCompany + `","customer_id":"` + testCustomer + `",` +
				`"session":1,"seq":3,"source_time_us":1767323045123456,"opened_at_us":1767323045123456,"lang":"en",` +
				`"customer_messages":1,"company_messages":1,` +
				`"message_id":"at://did:plc:testalice0000000000000000/app.bsky.feed.post/ccc333","direction":"company",` +
				`"response_time_us":90000250}`,
		},
		{
			name: "closed",
			in:   validConversation(TypeClosed),
			want: `{"type":"closed","channel":"bluesky","company_id":"` + testCompany + `","customer_id":"` + testCustomer + `",` +
				`"session":1,"seq":4,"source_time_us":1767323045123456,"opened_at_us":1767323045123456,"lang":"en",` +
				`"customer_messages":1,"company_messages":0,"close_reason":"inactivity"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := MarshalConversation(tt.in)
			if err != nil {
				t.Fatalf("MarshalConversation() error = %v", err)
			}
			if string(b) != tt.want {
				t.Errorf("MarshalConversation() =\n%s\nwant\n%s", b, tt.want)
			}
		})
	}
}

func TestConversationValidate(t *testing.T) {
	tests := []struct {
		name   string
		typ    ConversationType
		change func(c *Conversation)
		fields []string
	}{
		{name: "zero value", typ: TypeOpened, change: func(c *Conversation) { *c = Conversation{} }, fields: []string{
			"type", "channel", "company_id", "customer_id", "session", "seq", "source_time_us", "opened_at_us", "lang",
		}},
		{name: "company is customer", typ: TypeOpened, change: func(c *Conversation) { c.CustomerID = testCompany }, fields: []string{"customer_id"}},
		{name: "bad lang", typ: TypeOpened, change: func(c *Conversation) { c.Lang = "en-US" }, fields: []string{"lang"}},
		{name: "opened by company", typ: TypeOpened, change: func(c *Conversation) { c.Direction = FromCompany }, fields: []string{"direction"}},
		{name: "opened without message", typ: TypeOpened, change: func(c *Conversation) { c.MessageID, c.Direction = "", "" }, fields: []string{"message_id", "direction"}},
		{name: "added with unknown direction", typ: TypeMessageAdded, change: func(c *Conversation) { c.Direction = "bot" }, fields: []string{"direction"}},
		{name: "answered by customer", typ: TypeAnswered, change: func(c *Conversation) { c.Direction = FromCustomer }, fields: []string{"direction"}},
		{name: "negative response time", typ: TypeAnswered, change: func(c *Conversation) { c.ResponseTime = -time.Second }, fields: []string{"response_time_us"}},
		{name: "response time on opened", typ: TypeOpened, change: func(c *Conversation) { c.ResponseTime = time.Second }, fields: []string{"response_time_us"}},
		{name: "closed without reason", typ: TypeClosed, change: func(c *Conversation) { c.CloseReason = "" }, fields: []string{"close_reason"}},
		{name: "closed with unknown reason", typ: TypeClosed, change: func(c *Conversation) { c.CloseReason = "tired" }, fields: []string{"close_reason"}},
		{name: "closed with message", typ: TypeClosed, change: func(c *Conversation) { c.MessageID, c.Direction = testMsgID, FromCustomer }, fields: []string{"message_id", "direction"}},
		{name: "reason on added", typ: TypeMessageAdded, change: func(c *Conversation) { c.CloseReason = CloseInactivity }, fields: []string{"close_reason"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConversation(tt.typ)
			tt.change(&c)
			assertFieldErrors(t, c.Validate(), tt.fields...)
			_, err := MarshalConversation(c)
			assertFieldErrors(t, err, tt.fields...)
		})
	}

	t.Run("every close reason is valid", func(t *testing.T) {
		for _, r := range []CloseReason{CloseInactivity, CloseCustomerRemoved, CloseCompanyRemoved} {
			c := validConversation(TypeClosed)
			c.CloseReason = r
			if err := c.Validate(); err != nil {
				t.Errorf("close reason %q: Validate() error = %v", r, err)
			}
		}
	})
}

func TestUnmarshalConversation(t *testing.T) {
	closed := `{"type":"closed","channel":"bluesky","company_id":"a","customer_id":"b","session":2,"seq":7,` +
		`"source_time_us":1767323045123456,"opened_at_us":1767323045000000,"lang":"und",` +
		`"customer_messages":3,"company_messages":0,"close_reason":"customer_removed"`

	t.Run("unknown fields are ignored", func(t *testing.T) {
		c, err := UnmarshalConversation([]byte(closed + `,"operator":"x"}`))
		if err != nil {
			t.Fatalf("UnmarshalConversation() error = %v", err)
		}
		if c.CloseReason != CloseCustomerRemoved || !c.Reopened() || c.Seq != 7 {
			t.Errorf("UnmarshalConversation() = %+v", c)
		}
	})
	t.Run("unknown type wraps ErrUnknownKind", func(t *testing.T) {
		_, err := UnmarshalConversation([]byte(`{"type":"transferred"}`))
		if !errors.Is(err, ErrUnknownKind) {
			t.Errorf("UnmarshalConversation() error = %v, want ErrUnknownKind", err)
		}
	})

	bad := []struct {
		name   string
		value  string
		fields []string
	}{
		{name: "empty", value: ""},
		{name: "null", value: `null`, fields: []string{"type", "channel", "seq", "source_time_us", "lang"}},
		{name: "no type", value: `{"session":1}`, fields: []string{"type", "company_id", "opened_at_us"}},
		{name: "closed without seq", value: `{"type":"closed","channel":"bluesky","company_id":"a","customer_id":"b","session":1,` +
			`"source_time_us":1,"opened_at_us":1,"lang":"en","close_reason":"inactivity"}`, fields: []string{"seq"}},
		{name: "answered without response time", value: `{"type":"answered","channel":"bluesky","company_id":"a","customer_id":"b",` +
			`"session":1,"seq":2,"source_time_us":1,"opened_at_us":1,"lang":"en","message_id":"m1","direction":"company"}`,
			fields: []string{"response_time_us"}},
		{name: "negative counter", value: closed + `,"customer_messages":-1}`},
	}
	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalConversation([]byte(tt.value))
			assertFieldErrors(t, err, tt.fields...)
			if errors.Is(err, ErrUnknownKind) {
				t.Errorf("error %v wraps ErrUnknownKind, want a plain error", err)
			}
		})
	}
}
