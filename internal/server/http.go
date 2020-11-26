package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// NewHTTPServer takes address and runs HTTP server on that address

func NewHTTPServer (addr string) *http.Server {
	httpsrv := newHTTPServer() // Do not know about this
	
	r := mux.NewRouter() // Creates new router
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST") // Handles POST to root
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET") // Handles GET to root path

	return &http.Server {
		Addr: addr,
		Handler: r,
	}
	
}

type httpServer struct {
	Log *Log // Each server has a commpit log
}

// newHTTPServer is just a wrapper that returns a pointer to httpServer with new commit log
func newHTTPServer() *httpServer {
	return &httpServer {
		Log: NewLog(),
	}
}

//Contains the record that the caller wants to append to the commit log
type ProduceRequest struct {
	Record Record `json:"record"`
}

//Tells the caller at what offset the record was stored
type ProduceResponse struct {
	Offset uint64 `json:"offset`
}

//ConsumeRequest specifies the records that the caller wants to read
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

//ConsumeResponse contains the actual record that is going to be sent back to the caller
type ConsumeResponse struct {
	Record Record `json:"record"`
}

//handleProduce handles the writing of new logs to the commit log
func (s *httpServer) handleProduce (w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest

	err := json.NewDecoder(r.Body).Decode(&req) // Unmarshals the JSON HTTP body to the &req

	if err != nil {
		http.Error(w,err.Error(),http.StatusBadRequest) // Returns Bad request if error
		return
	}

	off,err := s.Log.Append(req.Record) // req is of type ProduceRequest which is type Record and type Record is a struct with Value and Offset vars

	if err != nil {
		http.Error(w,err.Error(),http.StatusInternalServerError) // returns internal server error if the particular log cannot be appended to the commit log
		return
	}

	res := ProduceResponse{Offset: off} // of is the actuall offset where the log was written

	err = json.NewEncoder(w).Encode(res) // Marshals the res into JSON

	if err != nil {
		http.Error(w,err.Error(),http.StatusInternalServerError) // Returns internal server error if there is an issue with the marshalling into JSON of res
		return
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {

	var req ConsumeRequest

	err := json.NewDecoder(r.Body).Decode(&req) //Unmarshalls the JSON body into  ConsumeRequest

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest) // Returns StatusBadRequest if unmarshaling fails
	}

	record, err := s.Log.Read(req.Offset) // Read is actually method of Log type, but since the httpserver type contains Log type, it can use Log method ;)

	if err == ErrOffsetNotFound {
		http.Error(w,err.Error(),http.StatusNotFound) // Returns NOT FOUND if the offset cannot be found in the commit log
		return
	}

	if err != nil {
		http.Error(w,err.Error(),http.StatusInternalServerError) // if something else goes wrong
		return
	}

	res := ConsumeResponse{Record: record} // If the record (log) is found, i return it

	err = json.NewEncoder(w).Encode(res) // Marshaling the record (log)

	if err != nil {
		http.Error(w,err.Error(),http.StatusInternalServerError) // if something else goes wrong
	}
	return
}