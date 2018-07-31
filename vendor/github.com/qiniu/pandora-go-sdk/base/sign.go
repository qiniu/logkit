package base

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

const qiniuHeaderPrefix = "X-Qiniu-"

var qiniuSubResource = []string{}

func Sign(ak, sk string, req *http.Request) error {
	sign, err := signRequest([]byte(sk), req)
	if err != nil {
		return err
	}

	auth := "Pandora " + ak + ":" + base64.URLEncoding.EncodeToString(sign)
	req.Header.Set(HTTPHeaderAuthorization, auth)

	return nil
}

func SignQiniuHeader(header http.Header) (out string) {
	var keys []string
	for key, _ := range header {
		if len(key) > len(qiniuHeaderPrefix) && key[:len(qiniuHeaderPrefix)] == qiniuHeaderPrefix {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return
	}

	if len(keys) > 1 {
		sort.Sort(sortKey(keys))
	}
	for _, key := range keys {
		out += "\n" + strings.ToLower(key) + ":" + header.Get(key)
	}
	return
}

func SignQiniuResource(url string, query url.Values) (out string) {
	out += url
	var keys []string
	for _, v := range qiniuSubResource {
		if query.Get(v) != "" {
			keys = append(keys, query.Get(v))
		}
	}
	if len(keys) == 0 {
		return
	}

	for i, k := range keys {
		if i == 0 {
			out += "?"
		}
		out += k + "=" + query.Get(k)
		if i != len(keys)-1 {
			out += "&"
		}
	}
	return
}

func signRequest(sk []byte, req *http.Request) ([]byte, error) {
	h := hmac.New(sha1.New, sk)

	io.WriteString(h,
		fmt.Sprintf("%s\n%s\n%s\n%s\n",
			req.Method,
			req.Header.Get("Content-MD5"),
			req.Header.Get("Content-Type"),
			req.Header.Get("Date")))

	io.WriteString(h, SignQiniuHeader(req.Header))
	io.WriteString(h, SignQiniuResource(req.URL.Path, req.URL.Query()))
	return h.Sum(nil), nil
}

type sortKey []string

func (p sortKey) Len() int           { return len(p) }
func (p sortKey) Less(i, j int) bool { return p[i] < p[j] }
func (p sortKey) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
