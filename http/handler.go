package http

import (
	"net/http"
	"strings"
)

// POST /servers/<ServerId>/action => []string{"POST", "servers", "*", "action"}
type Pattern []string

func (p Pattern) Match(method string, cmds []string) (args []string, ok bool) {

	if len(cmds)+1 != len(p) {
		return
	}

	if !strings.EqualFold(p[0], method) {
		return
	}

	for i := 1; i < len(p); i++ {
		if p[i] == "*" {
			args = append(args, cmds[i-1])
			continue
		}
		if !strings.EqualFold(p[i], cmds[i-1]) {
			return
		}
	}
	ok = true
	return
}

// "POST /servers/*/action"
func NewPattern(pattern string) Pattern {

	parts := strings.Split(pattern, "/")
	if method := parts[0]; strings.HasSuffix(method, " ") {
		parts[0] = method[:len(method)-1]
	}
	return parts
}

type route struct {
	pattern Pattern
	handler http.Handler
}

type ServeMux struct {
	routes []*route
	base   http.Handler
}

var DefaultServeMux = NewServeMux()

func NewServeMux() *ServeMux {

	return new(ServeMux)
}

func (h *ServeMux) SetDefault(handler http.Handler) {

	h.base = handler
}

func (h *ServeMux) handle(pattern Pattern, handler http.Handler) {

	h.routes = append(h.routes, &route{pattern, handler})
}

func (h *ServeMux) Handle(pattern string, handler http.Handler) {

	h.handle(NewPattern(pattern), handler)
}

func (h *ServeMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {

	h.handle(NewPattern(pattern), http.HandlerFunc(handler))
}

func (h *ServeMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	parts := strings.Split(r.URL.Path[1:], "/")

	for _, route := range h.routes {
		if args, ok := route.pattern.Match(r.Method, parts); ok {
			r.Header["*"] = args
			route.handler.ServeHTTP(w, r)
			return
		}
	}

	if h.base != nil {
		h.base.ServeHTTP(w, r)
	} else {
		http.NotFound(w, r)
	}
}
