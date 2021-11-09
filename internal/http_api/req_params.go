package http_api

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
)

// ReqParams http请求参数
type ReqParams struct {
	url.Values        // get请求参数
	Body       []byte // post请求参数
}

// NewReqParams 创建并解析req的请求参数
func NewReqParams(req *http.Request) (*ReqParams, error) {
	// 解析一个URL编码的查询字符串
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}

	// post请求
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	return &ReqParams{reqParams, data}, nil
}

func (r *ReqParams) Get(key string) (string, error) {
	v, ok := r.Values[key]
	if !ok {
		return "", errors.New("key not in query params")
	}
	return v[0], nil
}

func (r *ReqParams) GetAll(key string) ([]string, error) {
	v, ok := r.Values[key]
	if !ok {
		return nil, errors.New("key not in query params")
	}
	return v, nil
}
