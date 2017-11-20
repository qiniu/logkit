package base

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type TokenDesc struct {
	Url         string
	QueryString url.Values
	Expires     int64
	ContentMD5  string
	ContentType string
	Method      string
	Headers     http.Header
}

func (t *TokenDesc) AddQuery(k, v string) {
	if t.QueryString == nil {
		t.QueryString = url.Values{}
	}
	t.QueryString.Set(k, v)
}

func (t *TokenDesc) SetHeader(k, v string) {
	if t.Headers == nil {
		t.Headers = http.Header{}
	}
	t.Headers.Set(k, v)
}

func (t *TokenDesc) Validate() (err error) {
	if t.Url == "" {
		err = fmt.Errorf("url in token description should not be empty")
		return
	}
	if t.Method != MethodGet && t.Method != MethodPost && t.Method != MethodDelete && t.Method != MethodPut {
		err = fmt.Errorf("method should be one of \"GET\", \"PUT\", \"POST\" and \"DELETE\"")
		return
	}
	curr := time.Now().Unix()
	if t.Expires < curr {
		err = fmt.Errorf("token has been expired before making token")
		return
	}
	return
}

type tokenDesc struct {
	Resource    string `json:"resource"`
	Expires     int64  `json:"expires"`
	ContentMD5  string `json:"contentMD5"`
	ContentType string `json:"contentType"`
	Headers     string `json:"headers"`
	Method      string `json:"method"`
}

func newTokenDesc(t *TokenDesc) *tokenDesc {
	return &tokenDesc{
		Resource:    SignQiniuResource(t.Url, t.QueryString),
		Headers:     SignQiniuHeader(t.Headers),
		Method:      t.Method,
		ContentMD5:  t.ContentMD5,
		ContentType: t.ContentType,
		Expires:     t.Expires,
	}
}

func MakeTokenInternal(ak, sk string, desc *TokenDesc) (token string, err error) {
	if err = desc.Validate(); err != nil {
		return
	}

	td := newTokenDesc(desc)
	marshaledTokenDesc, err := json.Marshal(td)
	if err != nil {
		return
	}

	encodedTokenDesc := base64.URLEncoding.EncodeToString(marshaledTokenDesc)
	h := hmac.New(sha1.New, []byte(sk))
	io.WriteString(h, encodedTokenDesc)
	encodedSign := base64.URLEncoding.EncodeToString(h.Sum(nil))

	return fmt.Sprintf("Pandora %s:%s:%s", ak, encodedSign, encodedTokenDesc), nil
}
