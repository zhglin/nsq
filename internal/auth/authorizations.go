package auth

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/nsqio/nsq/internal/http_api"
)

type Authorization struct {
	Topic       string   `json:"topic"`       // 有权限的topic    正则
	Channels    []string `json:"channels"`    // 有权限的channel  正则
	Permissions []string `json:"permissions"` // 权限  subscribe || publish
}

// State 身份认证服务器的响应结果
type State struct {
	TTL            int             `json:"ttl"`            // 有效的时间
	Authorizations []Authorization `json:"authorizations"` // 权限
	Identity       string          `json:"identity"`
	IdentityURL    string          `json:"identity_url"`
	Expires        time.Time       // 根据ttl计算的过期时间
}

// HasPermission 是否有permission的授权
func (a *Authorization) HasPermission(permission string) bool {
	for _, p := range a.Permissions {
		if permission == p {
			return true
		}
	}
	return false
}

// IsAllowed 校验是否授权 必须topic，channel都匹配
func (a *Authorization) IsAllowed(topic, channel string) bool {
	if channel != "" {
		if !a.HasPermission("subscribe") {
			return false
		}
	} else {
		if !a.HasPermission("publish") {
			return false
		}
	}

	topicRegex := regexp.MustCompile(a.Topic)

	if !topicRegex.MatchString(topic) {
		return false
	}

	for _, c := range a.Channels {
		channelRegex := regexp.MustCompile(c)
		if channelRegex.MatchString(channel) {
			return true
		}
	}
	return false
}

// IsAllowed 授权校验
func (a *State) IsAllowed(topic, channel string) bool {
	for _, aa := range a.Authorizations {
		if aa.IsAllowed(topic, channel) {
			return true
		}
	}
	return false
}

// IsExpired 授权是否已过期
func (a *State) IsExpired() bool {
	if a.Expires.Before(time.Now()) {
		return true
	}
	return false
}

// QueryAnyAuthd 多次进行身份校验
func QueryAnyAuthd(authd []string, remoteIP string, tlsEnabled bool, commonName string, authSecret string,
	connectTimeout time.Duration, requestTimeout time.Duration) (*State, error) {
	start := rand.Int()
	n := len(authd)
	for i := 0; i < n; i++ {
		// 随机选地址
		a := authd[(i+start)%n]
		authState, err := QueryAuthd(a, remoteIP, tlsEnabled, commonName, authSecret, connectTimeout, requestTimeout)
		if err != nil {
			log.Printf("Error: failed auth against %s %s", a, err)
			continue
		}
		return authState, nil
	}
	return nil, errors.New("Unable to access auth server")
}

// QueryAuthd 发送http请求
func QueryAuthd(authd string, remoteIP string, tlsEnabled bool, commonName string, authSecret string,
	connectTimeout time.Duration, requestTimeout time.Duration) (*State, error) {
	v := url.Values{}
	v.Set("remote_ip", remoteIP) // 设置客户端地址
	if tlsEnabled {
		v.Set("tls", "true") // tls状态
	} else {
		v.Set("tls", "false")
	}
	v.Set("secret", authSecret) // 请求内容
	v.Set("common_name", commonName)

	var endpoint string
	if strings.Contains(authd, "://") {
		endpoint = fmt.Sprintf("%s?%s", authd, v.Encode())
	} else {
		endpoint = fmt.Sprintf("http://%s/auth?%s", authd, v.Encode())
	}

	var authState State
	client := http_api.NewClient(nil, connectTimeout, requestTimeout)
	if err := client.GETV1(endpoint, &authState); err != nil {
		return nil, err
	}

	// validation on response 验证响应
	for _, auth := range authState.Authorizations {
		for _, p := range auth.Permissions {
			switch p {
			case "subscribe", "publish": // 是否
			default:
				return nil, fmt.Errorf("unknown permission %s", p)
			}
		}

		if _, err := regexp.Compile(auth.Topic); err != nil {
			return nil, fmt.Errorf("unable to compile topic %q %s", auth.Topic, err)
		}

		for _, channel := range auth.Channels {
			if _, err := regexp.Compile(channel); err != nil {
				return nil, fmt.Errorf("unable to compile channel %q %s", channel, err)
			}
		}
	}

	if authState.TTL <= 0 {
		return nil, fmt.Errorf("invalid TTL %d (must be >0)", authState.TTL)
	}

	authState.Expires = time.Now().Add(time.Duration(authState.TTL) * time.Second)
	return &authState, nil
}
