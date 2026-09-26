package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

type Store interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	Delete(key string) error
	Close() error
}

type Cluster interface {
	Open()
	Get(key string) (string, error)
	Set(key string, value string) error
	Delete(key string) error
	AddNode() error
	RemoveNode(addr string)
	Close() error
}

type Service struct {
	addr    string
	ln      net.Listener
	mux     *http.ServeMux
	server  *http.Server
	cluster Cluster
}

// return unitialized HTTP service
func NewClusterService(addr string, cluster Cluster) *Service {
	return &Service{
		addr:    addr,
		cluster: cluster,
	}
}

func (s *Service) Start() error {
	s.mux = http.NewServeMux()

	s.mux.HandleFunc("/key/", s.handleKeyRequest)
	s.mux.HandleFunc("/key", s.handleKeyRequest)
	s.mux.HandleFunc("/add-node", s.handleAddNode)
	s.mux.HandleFunc("/remove-node/", s.handleNodeRemoval)

	s.server = &http.Server{
		Handler: s.mux,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("error listening on %s: %w", s.addr, err)
	}

	s.ln = ln

	go func() {
		err := s.server.Serve(s.ln)
		if err != nil && err != http.ErrServerClosed {
			log.Printf("error with the HTTP server error: %v", err)
		}
	}()

	return nil
}

func (s *Service) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Service) handleAddNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "err: must be POST method")
		return
	}

	if err := s.cluster.AddNode(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, fmt.Sprintf("error adding node: %v", err))
		return
	}

	w.WriteHeader(http.StatusCreated)
	io.WriteString(w, "node added successfully")
}

func (s *Service) handleNodeRemoval(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "err: must be POST method")
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 3 || parts[2] == "" {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "err: missing node address in path")
		return
	}

	s.cluster.RemoveNode(parts[2])
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, fmt.Sprintf("node %s removed successfully", parts[2]))
}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {
	getKey := func() (string, error) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 || parts[2] == "" {
			return "", fmt.Errorf("missing key in path")
		}
		return parts[2], nil
	}

	switch r.Method {
	case http.MethodPost:
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, fmt.Sprintf("error reading body: %v", err))
			return
		}

		m := map[string]string{}
		if err := json.Unmarshal(b, &m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, fmt.Sprintf("error parsing JSON: %v -- body was: %s", err, string(b)))
			return
		}

		if len(m) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, "request body contained no key-value pairs")
			return
		}

		for k, v := range m {
			if err := s.cluster.Set(k, v); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, fmt.Sprintf("error setting key %q: %v", k, err))
				return
			}
		}

		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, "keys stored successfully")

	case http.MethodGet:
		k, err := getKey()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, err.Error())
			return
		}

		val, err := s.cluster.Get(k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, fmt.Sprintf("error getting key %q: %v", k, err))
			return
		}

		w.WriteHeader(http.StatusOK)
		io.WriteString(w, val)

	case http.MethodDelete:
		k, err := getKey()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, err.Error())
			return
		}

		err = s.cluster.Delete(k)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, fmt.Sprintf("error deleting key %q: %v", k, err))
			return
		}

		w.WriteHeader(http.StatusOK)
		io.WriteString(w, fmt.Sprintf("deleted key %q successfully", k))

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, fmt.Sprintf("method %s not allowed", r.Method))
	}
}

func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}
