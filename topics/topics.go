// Package topics holds publisher-specific topic handling for the relay daemon.
//
// It mirrors websub_relay.topics so that clients can send the feed URL they
// already use instead of the topic form a particular hub expects.
package topics

import (
	"net/url"
	"sort"
	"strings"
)

// YouTubeRSSFeedBase is the base for the regularly published YouTube Atom feed.
const YouTubeRSSFeedBase = "https://www.youtube.com/feeds/"

// YouTubeWebSubFeedBase is the base for the feed form Google's WebSub service
// expects as a subscription topic.
const YouTubeWebSubFeedBase = "https://www.youtube.com/xml/feeds/"

// DefaultYouTubeHub is Google's public WebSub hub.
const DefaultYouTubeHub = "https://pubsubhubbub.appspot.com/"

var youtubeHosts = map[string]struct{}{
	"youtube.com":     {},
	"www.youtube.com": {},
	"m.youtube.com":   {},
}

const (
	youtubeRSSPath    = "/feeds/videos.xml"
	youtubeWebSubPath = "/xml/feeds/videos.xml"
)

// YouTubeRSSURL returns the YouTube Atom feed URL for a channel, using
// feedBaseURL as the base when one is provided.
func YouTubeRSSURL(channelID string, feedBaseURL string) string {
	base := feedBaseURL
	if base == "" {
		base = YouTubeRSSFeedBase
	}
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	parsed, err := url.Parse(base)
	if err != nil {
		return base + "videos.xml?channel_id=" + channelID
	}
	ref := &url.URL{Path: "videos.xml", RawQuery: "channel_id=" + url.QueryEscape(channelID)}
	return parsed.ResolveReference(ref).String()
}

// TranslateTopic rewrites a YouTube channel feed URL to the topic form Google's
// hub expects (/feeds/videos.xml -> /xml/feeds/videos.xml).  URLs that are
// already in the WebSub form, or that belong to another publisher, are returned
// unchanged.
func TranslateTopic(raw string) string {
	parts, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if _, ok := youtubeHosts[strings.ToLower(parts.Hostname())]; !ok {
		return raw
	}
	if parts.EscapedPath() != youtubeRSSPath {
		return raw
	}
	parts.Path = youtubeWebSubPath
	parts.RawPath = ""
	return parts.String()
}

// CanonicalizeTopic normalizes a topic URL so equivalent forms deduplicate to
// one hub subscription (lowercased scheme/host, default ports dropped, sorted
// query, fragment removed).
func CanonicalizeTopic(raw string) string {
	parts, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	scheme := strings.ToLower(parts.Scheme)
	host := strings.ToLower(parts.Hostname())
	port := parts.Port()
	if port != "" && !((scheme == "http" && port == "80") || (scheme == "https" && port == "443")) {
		host += ":" + port
	}
	path := parts.EscapedPath()
	if path == "" {
		path = "/"
	}
	return scheme + "://" + host + path + encodeQuerySorted(parts.RawQuery)
}

// encodeQuerySorted mirrors Python's urlencode(sorted(parse_qsl(...))): query
// pairs are sorted as (key, value) tuples and percent-encoded with quote_plus.
func encodeQuerySorted(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		// Fall back to the raw query if it cannot be parsed.
		return "?" + rawQuery
	}
	type pair struct{ key, value string }
	var pairs []pair
	for key, vals := range values {
		for _, value := range vals {
			pairs = append(pairs, pair{key, value})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key != pairs[j].key {
			return pairs[i].key < pairs[j].key
		}
		return pairs[i].value < pairs[j].value
	})
	var b strings.Builder
	b.WriteByte('?')
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(url.QueryEscape(p.key))
		b.WriteByte('=')
		b.WriteString(url.QueryEscape(p.value))
	}
	return b.String()
}

// NormalizeTopic normalizes a client-supplied topic to the form used for hub
// subscriptions.
func NormalizeTopic(raw string) string {
	return CanonicalizeTopic(TranslateTopic(raw))
}
