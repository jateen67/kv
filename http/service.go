package http

import (
	"net"
	"net/http"
)

type Store interface {
	Put(key string, value string) error
	Get(key string) (string, error)
	Close() bool
}

type Service struct {
	addr  string
	ln    net.Listener
	store Store
}

// return unitialized HTTP service
func NewService(addr string, store Store) *Service {
	return &Service{
		addr:  addr,
		store: store,
	}
}

func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.ln = ln
	http.Handle("/", s)

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			panic(err)
		}
	}()

	return nil
}

func (s *Service) Close() error {
	s.ln.Close()
	return nil
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	panic("implement this")
}

func (s *Service) handleKeyRequest(w http.ResponseWriter, r *http.Request) {

}

func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}
