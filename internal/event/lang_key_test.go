package event

import (
	"bytes"
	"testing"
)

func TestNormalizeLang(t *testing.T) {
	tests := []struct {
		name string
		tags []string
		want string
	}{
		{name: "plain", tags: []string{"en"}, want: "en"},
		{name: "region", tags: []string{"en-US"}, want: "en"},
		{name: "upper case", tags: []string{"EN"}, want: "en"},
		{name: "portuguese brazil", tags: []string{"pt-BR"}, want: "pt"},
		{name: "script subtag", tags: []string{"zh-Hant"}, want: "zh"},
		{name: "three letters", tags: []string{"fil"}, want: "fil"},
		{name: "underscore separator", tags: []string{"en_US"}, want: "en"},
		{name: "spaces around", tags: []string{"  de-AT "}, want: "de"},
		{name: "only the first tag counts", tags: []string{"ja", "en"}, want: "ja"},
		{name: "invalid first tag is und even with a valid second", tags: []string{"??", "en"}, want: "und"},
		{name: "nil list", tags: nil, want: "und"},
		{name: "empty list", tags: []string{}, want: "und"},
		{name: "empty string", tags: []string{""}, want: "und"},
		{name: "garbage", tags: []string{"%%%"}, want: "und"},
		{name: "one letter", tags: []string{"e"}, want: "und"},
		{name: "too long", tags: []string{"english"}, want: "und"},
		{name: "digits", tags: []string{"12"}, want: "und"},
		{name: "private use", tags: []string{"x-private"}, want: "und"},
		{name: "grandfathered", tags: []string{"i-klingon"}, want: "und"},
		{name: "non ascii letters", tags: []string{"\u00e9"}, want: "und"},
		{name: "leading separator", tags: []string{"-en"}, want: "und"},
		{name: "und stays und", tags: []string{"und"}, want: "und"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeLang(tt.tags)
			if got != tt.want {
				t.Errorf("NormalizeLang(%q) = %q, want %q", tt.tags, got, tt.want)
			}
			if !validLang(got) {
				t.Errorf("NormalizeLang(%q) = %q, which validLang rejects", tt.tags, got)
			}
		})
	}
}

func TestConversationKey(t *testing.T) {
	tests := []struct {
		name              string
		company, customer string
		want              string
	}{
		{name: "dids", company: testCompany, customer: testCustomer, want: testCompany + "|" + testCustomer},
		{name: "separator in company", company: "a|b", customer: "c", want: "a%7Cb|c"},
		{name: "separator in customer", company: "a", customer: "b|c", want: "a|b%7Cc"},
		{name: "escape character", company: "a%b", customer: "c%7Cd", want: "a%25b|c%257Cd"},
		{name: "only separators", company: "|", customer: "||", want: "%7C|%7C%7C"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := ConversationKey(tt.company, tt.customer)
			if string(key) != tt.want {
				t.Errorf("ConversationKey() = %q, want %q", key, tt.want)
			}
			company, customer, err := ParseConversationKey(key)
			if err != nil {
				t.Fatalf("ParseConversationKey(%q) error = %v", key, err)
			}
			if company != tt.company || customer != tt.customer {
				t.Errorf("ParseConversationKey(%q) = %q, %q, want %q, %q", key, company, customer, tt.company, tt.customer)
			}
		})
	}
}

func TestConversationKeyUnambiguous(t *testing.T) {
	pairs := [][2]string{
		{"a|b", "c"},
		{"a", "b|c"},
		{"a%7Cb", "c"},
		{"a%", "7Cb|c"},
		{"a", "b%7Cc"},
		{"a%25", "b"},
		{"a%", "25|b"},
	}
	seen := make(map[string][2]string)
	for _, p := range pairs {
		k := string(ConversationKey(p[0], p[1]))
		if prev, ok := seen[k]; ok {
			t.Errorf("pairs %q and %q share key %q", prev, p, k)
		}
		seen[k] = p
	}
}

func TestParseConversationKeyInvalid(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{name: "empty", key: ""},
		{name: "no separator", key: testCompany},
		{name: "two separators", key: "a|b|c"},
		{name: "empty company", key: "|b"},
		{name: "empty customer", key: "a|"},
		{name: "bare percent", key: "a%|b"},
		{name: "unknown escape", key: "a%3A|b"},
		{name: "short escape at end", key: "a|b%2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if c, u, err := ParseConversationKey([]byte(tt.key)); err == nil {
				t.Errorf("ParseConversationKey(%q) = %q, %q, want error", tt.key, c, u)
			}
		})
	}
}

func TestAuthorKey(t *testing.T) {
	if got := AuthorKey(testCustomer); !bytes.Equal(got, []byte(testCustomer)) {
		t.Errorf("AuthorKey() = %q, want %q", got, testCustomer)
	}
}
