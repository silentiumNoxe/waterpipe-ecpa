package cluster

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
)

func listen(
	wg *sync.WaitGroup,
	log *slog.Logger,
	stop <-chan struct{},
	addr *net.UDPAddr,
	onmessage func(payload []byte),
) error {
	var stopped = false
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-stop
		stopped = true
	}()

	socket, err := net.ListenUDP("udp", addr)
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

		var content = make([]byte, 1024)
		n, err := socket.Read(content)
		if err != nil {
			log.Warn("Unable to read request", "err", err)
			continue
		}

		if n < len(content) {
			onmessage(content[:n])
			continue
		}

		var buff bytes.Buffer
		buff.Write(content)
		for {
			content = make([]byte, 1024)
			n, err = socket.Read(content)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					log.Warn("Failed read payload")
					break
				}
			}

			buff.Write(content)
			if n < len(content) {
				onmessage(buff.Bytes())
				break
			}
		}
	}

	return nil
}
