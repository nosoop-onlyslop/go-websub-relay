package topics

import "testing"

func TestCanonicalizeTopic(t *testing.T) {
	cases := map[string]string{
		"HTTPS://Example.COM:443/feed?b=2&a=1#frag": "https://example.com/feed?a=1&b=2",
		"http://example.com:80/x":                   "http://example.com/x",
		"https://example.com:8443/x":                "https://example.com:8443/x",
	}
	for input, want := range cases {
		if got := CanonicalizeTopic(input); got != want {
			t.Errorf("CanonicalizeTopic(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestYouTubeRSSURL(t *testing.T) {
	if got := YouTubeRSSURL("UCabc", ""); got != "https://www.youtube.com/feeds/videos.xml?channel_id=UCabc" {
		t.Errorf("unexpected default feed url: %q", got)
	}
	if got := YouTubeRSSURL("UCabc", "https://proxy.example/feeds"); got != "https://proxy.example/feeds/videos.xml?channel_id=UCabc" {
		t.Errorf("unexpected proxied feed url: %q", got)
	}
}

func TestNormalizeTopic(t *testing.T) {
	cases := map[string]string{
		"https://www.youtube.com/feeds/videos.xml?channel_id=UCabc":          "https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCabc",
		"https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCabc":      "https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCabc",
		"https://feeds.example/xml/feeds/videos.xml?channel_id=UCabc":        "https://feeds.example/xml/feeds/videos.xml?channel_id=UCabc",
		"HTTPS://WWW.YouTube.COM:443/feeds/videos.xml?channel_id=UCabc#frag": "https://www.youtube.com/xml/feeds/videos.xml?channel_id=UCabc",
	}
	for input, want := range cases {
		if got := NormalizeTopic(input); got != want {
			t.Errorf("NormalizeTopic(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestTranslateTopicIsSelective(t *testing.T) {
	cases := []string{
		"https://www.youtube.com/feeds/other.xml",
		"https://notyoutube.example/feeds/videos.xml",
	}
	for _, input := range cases {
		if got := TranslateTopic(input); got != input {
			t.Errorf("TranslateTopic(%q) = %q, want unchanged", input, got)
		}
	}
}
