package handler

import (
	"io"
	"net/http"
)

//const (
//	statusUnprocessableEntity = 422
//	maxBodySize               = int64(10 << 20)
//)

type Logger interface {
	Info(v ...interface{})
}

type webhookHandler struct {
	log Logger
}

func New(logger Logger) http.Handler {
	return &webhookHandler{logger}
}

func (handler *webhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler.log.Info("Webhook received")

	var msg string
	switch r.FormValue("text")[10:] {
	case "Agreed?":
		msg = "Of course, my Lord!"
	default:
		msg = "Yes, my Lord?"
	}

	io.WriteString(w, fmt.Sprintf("{\"text\": \"%v\"}", msg))
}
