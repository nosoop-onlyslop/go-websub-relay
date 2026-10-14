// Package hub is a thin WebSub hub client: hub discovery, (re)subscribe and
// unsubscribe.  It mirrors websub_relay.hub.
package hub

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/nosoop-onlyslop/go-websub-relay/topics"
)

var (
	linkTag = regexp.MustCompile(`(?i)<link\b[^>]*>`)
	href    = regexp.MustCompile(`(?i)href\s*=\s*["']([^"']+)["']`)
	relHub  = regexp.MustCompile(`(?i)rel\s*=\s*["']?hub["']?`)
)

// DiscoverHub returns the hub advertised by topic, falling back to defaultHub
// (or the built-in default) when none is found.
func DiscoverHub(ctx context.Context, client *http.Client, topic, defaultHub string) string {
	if defaultHub != "" {
		return defaultHub
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, topic, nil)
	if err != nil {
		return topics.DefaultYouTubeHub
	}
	request.Header.Set("Accept", "application/atom+xml, text/html")
	response, err := client.Do(request)
	if err != nil {
		slog.Warn("hub discovery failed", "topic", topic, "error", err)
		return topics.DefaultYouTubeHub
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		slog.Warn("hub discovery failed", "topic", topic, "status", response.Status)
		return topics.DefaultYouTubeHub
	}
	if response.Request != nil && response.Request.URL != nil {
		topic = response.Request.URL.String()
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		slog.Warn("hub discovery failed", "topic", topic, "error", err)
		return topics.DefaultYouTubeHub
	}
	for _, tag := range linkTag.FindAllString(string(body), -1) {
		if !relHub.MatchString(tag) {
			continue
		}
		match := href.FindStringSubmatch(tag)
		if match == nil {
			continue
		}
		base, err := url.Parse(topic)
		if err != nil {
			return match[1]
		}
		reference, err := url.Parse(match[1])
		if err != nil {
			return match[1]
		}
		return base.ResolveReference(reference).String()
	}
	return topics.DefaultYouTubeHub
}

// Subscribe sends a hub.mode=subscribe request.
func Subscribe(
	ctx context.Context,
	client *http.Client,
	hubURL, callback, topic, secret string,
	leaseSeconds int,
) (*http.Response, error) {
	form := url.Values{
		"hub.mode":          {"subscribe"},
		"hub.callback":      {callback},
		"hub.topic":         {topic},
		"hub.secret":        {secret},
		"hub.lease_seconds": {strconv.Itoa(leaseSeconds)},
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, hubURL, strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return client.Do(request)
}

// Unsubscribe sends a hub.mode=unsubscribe request.
func Unsubscribe(
	ctx context.Context,
	client *http.Client,
	hubURL, callback, topic string,
) (*http.Response, error) {
	form := url.Values{
		"hub.mode":     {"unsubscribe"},
		"hub.callback": {callback},
		"hub.topic":    {topic},
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, hubURL, strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return client.Do(request)
}
