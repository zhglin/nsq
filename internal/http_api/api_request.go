package http_api

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// NewDeadlineTransport A custom http.Transport with support for deadline timeouts
// 一个自定义http。支持最后期限超时的传输
func NewDeadlineTransport(connectTimeout time.Duration, requestTimeout time.Duration) *http.Transport {
	// arbitrary values copied from http.DefaultTransport
	transport := &http.Transport{
		// 指定创建TCP连接的拨号函数。
		DialContext: (&net.Dialer{
			Timeout:   connectTimeout,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		// 等待接收服务端的回复的头域的最大时间。零值表示不设置超时。该时间不包括获取回复主体的时间。
		ResponseHeaderTimeout: requestTimeout,
		// 每个主机下的最大闲置连接。
		MaxIdleConns: 100,
		// 链接的最大空闲时间
		IdleConnTimeout: 90 * time.Second,
		// 指定等待TLS握手的最大时间
		TLSHandshakeTimeout: 10 * time.Second,
	}
	return transport
}

type Client struct {
	c *http.Client
}

func NewClient(tlsConfig *tls.Config, connectTimeout time.Duration, requestTimeout time.Duration) *Client {
	transport := NewDeadlineTransport(connectTimeout, requestTimeout)
	transport.TLSClientConfig = tlsConfig
	return &Client{
		c: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout,
		},
	}
}

// GETV1 is a helper function to perform a V1 HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
func (c *Client) GETV1(endpoint string, v interface{}) error {
retry:
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}
	err = json.Unmarshal(body, &v)
	if err != nil {
		return err
	}

	return nil
}

// PostV1 is a helper function to perform a V1 HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
func (c *Client) POSTV1(endpoint string) error {
retry:
	req, err := http.NewRequest("POST", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}

	return nil
}

func httpsEndpoint(endpoint string, body []byte) (string, error) {
	var forbiddenResp struct {
		HTTPSPort int `json:"https_port"`
	}
	err := json.Unmarshal(body, &forbiddenResp)
	if err != nil {
		return "", err
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}

	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return "", err
	}

	u.Scheme = "https"
	u.Host = net.JoinHostPort(host, strconv.Itoa(forbiddenResp.HTTPSPort))
	return u.String(), nil
}
