package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
)

type acceptFn func(req *request, w io.Writer) error

type request struct {
	Addr    string
	Payload []byte
}

func listen(ctx context.Context, wg *sync.WaitGroup, log *slog.Logger, port string, accept acceptFn) error {
	if port == "" {
		return fmt.Errorf("port is empty")
	}

	var stopped bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Info("Shutdown cluster server")
		stopped = true
	}()

	socket, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	defer func() {
		_ = socket.Close()
	}()

	for {
		if stopped {
			break
		}

		conn, err := socket.Accept()
		if err != nil {
			log.Warn("failed to accept connection", "err", err)
		}
		wg.Add(1)
		go func(conn net.Conn) {
			defer func() {
				_ = conn.Close()
			}()

			addr := conn.RemoteAddr().String()
			payload, err := io.ReadAll(conn) // todo: firstly need to read content size
			if err != nil {
				log.Warn("Unable to read request", "err", err)
				return
			}

			err = accept(&request{Addr: addr, Payload: payload}, conn)
			if err != nil {
				log.Warn("Accepting message failed", "err", err)
				return
			}
		}(conn)
	}

	return nil
}
