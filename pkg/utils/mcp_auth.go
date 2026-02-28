package utils

import (
	"fmt"
	"net/url"
	"strings"
)

// MCPAuthNormalizationResult carries normalized endpoint/header output for MCP calls.
// The Endpoint field is the sanitized URL, Headers contains normalized HTTP headers,
// MovedQueryCredentials reports whether query credentials were converted to Authorization,
// and StrippedSensitiveQuery reports whether sensitive query keys were removed.
type MCPAuthNormalizationResult struct {
	Endpoint               string
	Headers                map[string]string
	MovedQueryCredentials  bool
	StrippedSensitiveQuery bool
}

// NormalizeMCPRemoteEndpointAuth sanitizes MCP endpoint auth by preferring Authorization headers.
// The endpoint parameter may include legacy query credentials (for example APIKEY/api_key),
// and headers provides optional caller-supplied HTTP headers.
// It returns sanitized endpoint/headers while never exposing sensitive values in output flags.
func NormalizeMCPRemoteEndpointAuth(endpoint string, headers map[string]string) MCPAuthNormalizationResult {
	normalizedEndpoint := strings.TrimSpace(endpoint)
	normalizedHeaders := normalizeMCPHeaders(headers)

	parsed, err := url.Parse(normalizedEndpoint)
	if err != nil {
		return MCPAuthNormalizationResult{
			Endpoint: normalizedEndpoint,
			Headers:  normalizedHeaders,
		}
	}

	query := parsed.Query()
	sensitiveKeys := findSensitiveQueryKeys(query)
	credentialFromQuery := firstSensitiveQueryValue(query, sensitiveKeys)

	strippedSensitiveQuery := len(sensitiveKeys) > 0
	for _, key := range sensitiveKeys {
		query.Del(key)
	}
	parsed.RawQuery = query.Encode()
	normalizedEndpoint = parsed.String()

	movedQueryCredentials := false
	if credentialFromQuery != "" && !hasMCPAuthorizationHeader(normalizedHeaders) {
		normalizedHeaders["Authorization"] = normalizeBearerCredential(credentialFromQuery)
		movedQueryCredentials = true
	}

	return MCPAuthNormalizationResult{
		Endpoint:               normalizedEndpoint,
		Headers:                normalizedHeaders,
		MovedQueryCredentials:  movedQueryCredentials,
		StrippedSensitiveQuery: strippedSensitiveQuery,
	}
}

// normalizeMCPHeaders trims and canonicalizes header keys and values.
// The headers parameter may be nil and return value is always a non-nil map.
func normalizeMCPHeaders(headers map[string]string) map[string]string {
	normalized := map[string]string{}
	for rawKey, rawValue := range headers {
		headerKey := strings.TrimSpace(rawKey)
		if headerKey == "" {
			continue
		}
		normalized[headerKey] = strings.TrimSpace(rawValue)
	}

	return normalized
}

// findSensitiveQueryKeys collects query keys that should never carry MCP credentials.
// The values parameter is parsed URL query data and return value keeps original key casing.
func findSensitiveQueryKeys(values url.Values) []string {
	if len(values) == 0 {
		return nil
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		trimmedKey := strings.TrimSpace(key)
		switch {
		case strings.EqualFold(trimmedKey, "apikey"):
			keys = append(keys, key)
		case strings.EqualFold(trimmedKey, "api_key"):
			keys = append(keys, key)
		case strings.EqualFold(trimmedKey, "authorization"):
			keys = append(keys, key)
		}
	}

	return keys
}

// firstSensitiveQueryValue returns the first non-empty sensitive query value.
// The values parameter is parsed URL query data and keys enumerates sensitive keys.
// It returns an empty string when no usable value exists.
func firstSensitiveQueryValue(values url.Values, keys []string) string {
	for _, key := range keys {
		for _, rawValue := range values[key] {
			trimmedValue := strings.TrimSpace(rawValue)
			if trimmedValue != "" {
				return trimmedValue
			}
		}
	}

	return ""
}

// hasMCPAuthorizationHeader checks whether non-empty Authorization header is already present.
// The headers parameter is an HTTP header map and return value is true when auth already exists.
func hasMCPAuthorizationHeader(headers map[string]string) bool {
	for key, value := range headers {
		if strings.EqualFold(strings.TrimSpace(key), "authorization") && strings.TrimSpace(value) != "" {
			return true
		}
	}

	return false
}

// normalizeBearerCredential ensures Authorization header content has Bearer prefix.
// The credential parameter is a raw token or existing Authorization value.
// It returns a normalized header value.
func normalizeBearerCredential(credential string) string {
	trimmedCredential := strings.TrimSpace(credential)
	if trimmedCredential == "" {
		return ""
	}

	if strings.HasPrefix(strings.ToLower(trimmedCredential), "bearer ") {
		return trimmedCredential
	}

	return fmt.Sprintf("Bearer %s", trimmedCredential)
}
