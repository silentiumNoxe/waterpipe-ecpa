package cluster

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func publish(target string, payload []byte) error {
	s := strings.Split(target, ":")
	domain := s[0]
	port := s[1]

	ip := net.ParseIP(domain)
	if ip == nil {
		ips, err := net.LookupIP(domain)
		if err != nil {
			return err
		}
		if len(ips) == 0 {
			return fmt.Errorf("no ip found for domain %s", domain)
		}
		ip = ips[0]
	}

	raddr := &net.UDPAddr{IP: ip}
	if port != "" {
		p, err := strconv.Atoi(port)
		if err != nil {
			return err
		}
		raddr.Port = p
	}

	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	_, err = conn.Write(payload)
	if err != nil {
		return err
	}

	return nil
}
