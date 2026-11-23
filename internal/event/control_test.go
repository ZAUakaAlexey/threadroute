package event

import (
	"errors"
	"reflect"
	"testing"
)

const testRemovalID = "a:did:plc:testbob000000000000000000:0:deleted:2026-01-02T03:04:05.123Z"

func validDeletion() Deletion {
	return Deletion{
		EventID:    "c:did:plc:testbob000000000000000000:app.bsky.feed.post:aaa111:3testrev00002:delete",
		Channel:    ChannelBluesky,
		SourceTime: testTime,
		MessageID:  testMsgID,
		AuthorID:   testCustomer,
	}
}

func validRemoval() AuthorRemoval {
	return AuthorRemoval{
		EventID:    testRemovalID,
		Channel:    ChannelBluesky,
		SourceTime: testTime,
		AuthorID:   testCustomer,
		Reason:     RemovalInactive,
		Status:     "deleted",
	}
}

func TestControlRoundTrip(t *testing.T) {
	noStatus := validRemoval()
	noStatus.Status = ""
	sync := validRemoval()
	sync.Reason, sync.Status = RemovalSync, ""

	tests := []struct {
		name string
		in   Control
		kind ControlKind
	}{
		{name: "deletion", in: validDeletion(), kind: KindDeletion},
		{name: "removal inactive", in: validRemoval(), kind: KindAuthorRemoval},
		{name: "removal inactive without status", in: noStatus, kind: KindAuthorRemoval},
		{name: "removal sync", in: sync, kind: KindAuthorRemoval},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := MarshalControl(tt.in)
			if err != nil {
				t.Fatalf("MarshalControl() error = %v", err)
			}
			out, err := UnmarshalControl(b)
			if err != nil {
				t.Fatalf("UnmarshalControl() error = %v", err)
			}
			if !reflect.DeepEqual(out, tt.in) {
				t.Errorf("round trip:\n got %#v\nwant %#v", out, tt.in)
			}
			if out.Kind() != tt.kind {
				t.Errorf("Kind() = %q, want %q", out.Kind(), tt.kind)
			}
			if out.Author() != testCustomer {
				t.Errorf("Author() = %q, want %q", out.Author(), testCustomer)
			}
		})
	}
}

func TestMarshalControlPointer(t *testing.T) {
	d := validDeletion()
	b, err := MarshalControl(&d)
	if err != nil {
		t.Fatalf("MarshalControl(&Deletion) error = %v", err)
	}
	want, _ := MarshalControl(d)
	if string(b) != string(want) {
		t.Errorf("MarshalControl(&d) = %s, want %s", b, want)
	}

	var nilDeletion *Deletion
	var nilRemoval *AuthorRemoval
	for _, c := range []Control{nil, nilDeletion, nilRemoval} {
		if _, err := MarshalControl(c); err == nil {
			t.Errorf("MarshalControl(%#v) error = nil, want error", c)
		}
	}
}

func TestMarshalControlFormat(t *testing.T) {
	tests := []struct {
		name string
		in   Control
		want string
	}{
		{
			name: "deletion",
			in:   validDeletion(),
			want: `{"kind":"deletion","event_id":"c:did:plc:testbob000000000000000000:app.bsky.feed.post:aaa111:3testrev00002:delete",` +
				`"channel":"bluesky","source_time_us":1767323045123456,"message_id":"` + testMsgID + `","author_id":"` + testCustomer + `"}`,
		},
		{
			name: "author removal",
			in:   validRemoval(),
			want: `{"kind":"author_removal","event_id":"` + testRemovalID + `","channel":"bluesky","source_time_us":1767323045123456,` +
				`"author_id":"` + testCustomer + `","reason":"inactive","status":"deleted"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := MarshalControl(tt.in)
			if err != nil {
				t.Fatalf("MarshalControl() error = %v", err)
			}
			if string(b) != tt.want {
				t.Errorf("MarshalControl() =\n%s\nwant\n%s", b, tt.want)
			}
		})
	}
}

func TestControlValidate(t *testing.T) {
	tests := []struct {
		name   string
		in     Control
		fields []string
	}{
		{name: "empty deletion", in: Deletion{}, fields: []string{"event_id", "channel", "source_time_us", "message_id", "author_id"}},
		{name: "empty removal", in: AuthorRemoval{}, fields: []string{"event_id", "channel", "source_time_us", "author_id", "reason"}},
		{name: "unknown reason", in: AuthorRemoval{
			EventID: testRemovalID, Channel: ChannelBluesky, SourceTime: testTime, AuthorID: testCustomer, Reason: "bored",
		}, fields: []string{"reason"}},
		{name: "sync with status", in: AuthorRemoval{
			EventID: testRemovalID, Channel: ChannelBluesky, SourceTime: testTime, AuthorID: testCustomer, Reason: RemovalSync, Status: "deleted",
		}, fields: []string{"status"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertFieldErrors(t, tt.in.Validate(), tt.fields...)
			_, err := MarshalControl(tt.in)
			assertFieldErrors(t, err, tt.fields...)
		})
	}
}

func TestUnmarshalControl(t *testing.T) {
	t.Run("unknown fields are ignored", func(t *testing.T) {
		b := `{"kind":"deletion","event_id":"e1","channel":"bluesky","source_time_us":1767323045123456,` +
			`"message_id":"m1","author_id":"` + testCustomer + `","extra":[1,2]}`
		c, err := UnmarshalControl([]byte(b))
		if err != nil {
			t.Fatalf("UnmarshalControl() error = %v", err)
		}
		d, ok := c.(Deletion)
		if !ok || d.MessageID != "m1" || !d.SourceTime.Equal(testTime) {
			t.Errorf("UnmarshalControl() = %#v", c)
		}
	})
	t.Run("unknown kind wraps ErrUnknownKind", func(t *testing.T) {
		_, err := UnmarshalControl([]byte(`{"kind":"label","event_id":"e1"}`))
		if !errors.Is(err, ErrUnknownKind) {
			t.Errorf("UnmarshalControl() error = %v, want ErrUnknownKind", err)
		}
	})

	bad := []struct {
		name   string
		value  string
		fields []string
	}{
		{name: "empty", value: ""},
		{name: "null", value: `null`, fields: []string{"kind"}},
		{name: "no kind", value: `{"event_id":"e1","author_id":"a"}`, fields: []string{"kind"}},
		{name: "deletion without message", value: `{"kind":"deletion","event_id":"e1","channel":"bluesky","source_time_us":1,"author_id":"a"}`,
			fields: []string{"message_id"}},
		{name: "removal without reason", value: `{"kind":"author_removal","event_id":"e1","channel":"bluesky","source_time_us":1,"author_id":"a"}`,
			fields: []string{"reason"}},
		{name: "wrong type", value: `{"kind":"deletion","source_time_us":true}`},
	}
	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalControl([]byte(tt.value))
			assertFieldErrors(t, err, tt.fields...)
			if errors.Is(err, ErrUnknownKind) {
				t.Errorf("error %v wraps ErrUnknownKind, want a plain error", err)
			}
		})
	}
}
