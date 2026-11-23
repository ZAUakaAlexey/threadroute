package event

import (
	"reflect"
	"testing"
	"time"
)

func validMessage() Message {
	return Message{
		EventID:    testEventID,
		Channel:    ChannelBluesky,
		SourceTime: testTime,
		MessageID:  testMsgID,
		CompanyID:  testCompany,
		CustomerID: testCustomer,
		Direction:  FromCustomer,
		ParentID:   testParentID,
		Text:       testText,
		Langs:      []string{"en-US", "de"},
		Lang:       "en",
	}
}

func TestMessageRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		change func(m *Message)
	}{
		{name: "customer message"},
		{name: "company message", change: func(m *Message) { m.Direction = FromCompany }},
		{name: "empty text", change: func(m *Message) { m.Text = "" }},
		{name: "no langs", change: func(m *Message) { m.Langs = []string{}; m.Lang = LangUndetermined }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := validMessage()
			if tt.change != nil {
				tt.change(&in)
			}
			b, err := MarshalMessage(in)
			if err != nil {
				t.Fatalf("MarshalMessage() error = %v", err)
			}
			out, err := UnmarshalMessage(b)
			if err != nil {
				t.Fatalf("UnmarshalMessage() error = %v", err)
			}
			if !reflect.DeepEqual(out, in) {
				t.Errorf("round trip:\n got %+v\nwant %+v", out, in)
			}
		})
	}
}

func TestMarshalMessageFormat(t *testing.T) {
	m := validMessage()
	m.Langs = nil
	m.Lang = LangUndetermined
	b, err := MarshalMessage(m)
	if err != nil {
		t.Fatalf("MarshalMessage() error = %v", err)
	}
	want := `{"event_id":"` + testEventID + `","channel":"bluesky","source_time_us":1767323045123456,` +
		`"message_id":"` + testMsgID + `","company_id":"` + testCompany + `","customer_id":"` + testCustomer + `",` +
		`"direction":"customer","parent_id":"` + testParentID + `","text":"synthetic reply one","langs":[],"lang":"und"}`
	if string(b) != want {
		t.Errorf("MarshalMessage() =\n%s\nwant\n%s", b, want)
	}
}

func TestMessageValidate(t *testing.T) {
	tests := []struct {
		name   string
		change func(m *Message)
		fields []string
	}{
		{name: "zero value", change: func(m *Message) { *m = Message{} }, fields: []string{
			"event_id", "channel", "source_time_us", "message_id", "company_id", "customer_id", "direction", "parent_id", "lang",
		}},
		{name: "source time at unix zero", change: func(m *Message) { m.SourceTime = time.UnixMicro(0) }, fields: []string{"source_time_us"}},
		{name: "company is customer", change: func(m *Message) { m.CustomerID = testCompany }, fields: []string{"customer_id"}},
		{name: "unknown direction", change: func(m *Message) { m.Direction = "operator" }, fields: []string{"direction"}},
		{name: "parent is the message", change: func(m *Message) { m.ParentID = testMsgID }, fields: []string{"parent_id"}},
		{name: "lang upper case", change: func(m *Message) { m.Lang = "EN" }, fields: []string{"lang"}},
		{name: "lang with region", change: func(m *Message) { m.Lang = "en-us" }, fields: []string{"lang"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := validMessage()
			tt.change(&m)
			assertFieldErrors(t, m.Validate(), tt.fields...)
			_, err := MarshalMessage(m)
			assertFieldErrors(t, err, tt.fields...)
		})
	}
}

func TestUnmarshalMessage(t *testing.T) {
	valid := `{"event_id":"e1","channel":"bluesky","source_time_us":1767323045123456,` +
		`"message_id":"m1","company_id":"` + testCompany + `","customer_id":"` + testCustomer + `",` +
		`"direction":"company","parent_id":"m0","text":"synthetic reply one","langs":["pt-BR"],"lang":"pt"}`

	t.Run("unknown fields are ignored", func(t *testing.T) {
		b := valid[:len(valid)-1] + `,"new_field":{"x":1}}`
		m, err := UnmarshalMessage([]byte(b))
		if err != nil {
			t.Fatalf("UnmarshalMessage() error = %v", err)
		}
		if m.Direction != FromCompany || m.Lang != "pt" || !m.SourceTime.Equal(testTime) {
			t.Errorf("UnmarshalMessage() = %+v", m)
		}
	})
	t.Run("missing langs gives empty list", func(t *testing.T) {
		b := `{"event_id":"e1","channel":"bluesky","source_time_us":1,"message_id":"m1","company_id":"a","customer_id":"b",` +
			`"direction":"customer","parent_id":"m0","text":"","lang":"und"}`
		m, err := UnmarshalMessage([]byte(b))
		if err != nil {
			t.Fatalf("UnmarshalMessage() error = %v", err)
		}
		if m.Langs == nil || len(m.Langs) != 0 {
			t.Errorf("Langs = %#v, want empty list", m.Langs)
		}
	})

	bad := []struct {
		name   string
		value  string
		fields []string
	}{
		{name: "empty", value: ""},
		{name: "null", value: `null`, fields: []string{"event_id", "source_time_us", "lang"}},
		{name: "empty object", value: `{}`, fields: []string{"event_id", "channel", "company_id", "direction"}},
		{name: "not json", value: `synthetic reply one`},
		{name: "wrong type", value: `{"source_time_us":"soon"}`},
		{name: "negative time", value: `{"event_id":"e1","channel":"bluesky","source_time_us":-5,"message_id":"m1","company_id":"a",` +
			`"customer_id":"b","direction":"customer","parent_id":"m0","text":"synthetic reply one","lang":"en"}`, fields: []string{"source_time_us"}},
	}
	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalMessage([]byte(tt.value))
			assertFieldErrors(t, err, tt.fields...)
		})
	}
}
