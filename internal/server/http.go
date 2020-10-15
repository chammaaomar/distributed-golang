package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// ProduceRequest represents the schema of JSON requests acceptable by the
// POST endpoint
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse represents the schema of the JSON response from the POST endpoint
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest represents the schema of the JSON request acceptable by the GET endpoint
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse represents the schema of the JSON response from the GET endpoint
type ConsumeResponse struct {
	Record Record `json:"record"`
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{Log: NewLog()}
}

func (server *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	// deserialize json into struct
	// do stuff
	// serialize struct back into json
	// write response
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := server.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := ProduceResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (server *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := server.Log.Read(req.Offset)
	if err == ErrIllegalOffset {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// NewHTTPServer returns an http server with two endpoints
// one for writing to the commit log, and the other for reading from it
func NewHTTPServer(addr string) *http.Server {
	server := newHTTPServer()
	router := mux.NewRouter()
	router.HandleFunc("/", server.handleProduce).Methods("POST")
	router.HandleFunc("/", server.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}
