package datahub

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

func newHttpClient(server string, accessToken string) *httpClient {
	client := &httpClient{}
	client.server = server
	client.accessToken = accessToken
	client.timeout = 30 * time.Second
	return client
}

func (client *httpClient) withUserAgent(userAgent string) *httpClient {
	client.userAgent = userAgent
	return client
}

type httpClient struct {
	userAgent   string
	server      string
	accessToken string
	timeout     time.Duration
}

type httpVerb string

const (
	httpGet    httpVerb = "GET"
	httpPost   httpVerb = "POST"
	httpPut    httpVerb = "PUT"
	httpDelete httpVerb = "DELETE"
)

func (client *httpClient) makeRequest(method httpVerb, path string, content []byte, headers map[string]string, queryParams map[string]string) ([]byte, error) {
	resp, err := client.makeStreamingRequest(method, path, content, headers, queryParams)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = resp.Close()
	}()

	bodyBytes, err := io.ReadAll(resp)
	if err != nil {
		return nil, err
	}

	return bodyBytes, nil
}

func (client *httpClient) makeStreamingRequest(method httpVerb, path string, content []byte, headers map[string]string, queryParams map[string]string) (io.ReadCloser, error) {
	baseURL := fmt.Sprintf("%s%s", client.server, path)
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	// Prepare the query parameters.
	if queryParams != nil {
		values := url.Values{}
		for key, value := range queryParams {
			values.Add(key, value)
		}

		// Encode the parameters and append to the URL.
		parsedURL.RawQuery = values.Encode()
	}
	fullUrl := parsedURL.String()

	req, err := http.NewRequest(string(method), fullUrl, bytes.NewBuffer(content))
	if err != nil {
		return nil, err
	}

	if client.accessToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", client.accessToken))
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", client.userAgent)
	if headers != nil {
		for key, val := range headers {
			req.Header.Set(key, val)
		}
	}

	c := http.Client{
		Timeout: client.timeout,
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return resp.Body, nil
	} else {
		return nil, errors.New("error in request http status " + resp.Status)
	}
}

func (client *httpClient) makeStreamingWriterRequest(method httpVerb, path string, writeBody func(writer io.Writer) error, headers map[string]string, queryParams map[string]string) (io.ReadCloser, error) {
	baseURL := fmt.Sprintf("%s%s", client.server, path)
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	// Prepare the query parameters.
	if queryParams != nil {
		values := url.Values{}
		for key, value := range queryParams {
			values.Add(key, value)
		}

		// Encode the parameters and append to the URL.
		parsedURL.RawQuery = values.Encode()
	}
	fullUrl := parsedURL.String()

	reader, writer := io.Pipe()
	req, err := http.NewRequest(string(method), fullUrl, reader)
	if err != nil {
		return nil, err
	}

	if client.accessToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", client.accessToken))
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", client.userAgent)

	if headers != nil {
		for key, val := range headers {
			req.Header.Set(key, val)
		}
	}

	c := http.Client{
		Timeout: client.timeout,
	}

	go func() {
		defer writer.Close()
		writeBody(writer)
	}()

	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return resp.Body, nil
	} else {
		resp.Body.Close()
		return nil, errors.New("error in request http status " + resp.Status)
	}
}
