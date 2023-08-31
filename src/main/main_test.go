package main

import "testing"

func TestParseHostPort(t *testing.T) {
	checkParseHostPort(t, "localhost", 5432, "localhost", 5432, false)
	checkParseHostPort(t, "localhost:54333", 5432, "localhost", 54333, false)
	checkParseHostPort(t, "localhost:555433", 5432, "localhost", 5432, true)
	checkParseHostPort(t, "localhost:eee", 5432, "localhost", 5432, true)
}

func checkParseHostPort(t *testing.T, hostPort string, defaultPort int, expectedHost string, expectedPort int, errorExpected bool) {
	host, port, err := parseHostPort(hostPort, defaultPort)
	if err == nil {
		if errorExpected {
			t.Fatal("error expected")
		}
		if host != expectedHost {
			t.Fatal("wrong host")
		}
		if port != expectedPort {
			t.Fatal("wrong port")
		}
	} else {
		if !errorExpected {
			t.Fatal("unexpected error")
		}
	}
}
