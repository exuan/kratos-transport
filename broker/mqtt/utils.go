package mqtt

import (
	"fmt"
	"strconv"
	"strings"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

var clientIdPrefix = "kratos_server_"

// setAddrs set the mqtt server address
func setAddrs(addrs []string) []string {
	cAddrs := make([]string, 0, len(addrs))

	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}

		var scheme string
		var host string
		var port int

		// split on scheme
		parts := strings.Split(addr, "://")

		// no scheme
		if len(parts) < 2 {
			// default tcp scheme
			scheme = "tcp"
			parts = strings.Split(parts[0], ":")
			// got scheme
		} else {
			scheme = parts[0]
			parts = strings.Split(parts[1], ":")
		}

		// no parts
		if len(parts) == 0 {
			continue
		}

		// check scheme
		switch scheme {
		case "tcp", "ssl", "ws":
		default:
			continue
		}

		if len(parts) < 2 {
			// no port
			host = parts[0]

			switch scheme {
			case "tcp":
				port = 1883
			case "ssl":
				port = 8883
			case "ws":
				// support secure port
				port = 80
			default:
				port = 1883
			}
			// got host port
		} else {
			host = parts[0]
			port, _ = strconv.Atoi(parts[1])
		}

		addr = fmt.Sprintf("%s://%s:%d", scheme, host, port)
		cAddrs = append(cAddrs, addr)

	}

	// default an address if we have none
	if len(cAddrs) == 0 {
		cAddrs = []string{"tcp://127.0.0.1:1883"}
	}

	return cAddrs
}

// generateClientId generate the client id
func generateClientId() string {
	return fmt.Sprintf("%s_%s", clientIdPrefix, uuid.New().String())
}

// checkClientToken check the client token
func checkClientToken(token paho.Token) (bool, error) {
	if token.Wait() && token.Error() != nil {
		return false, token.Error()
	}
	return true, nil
}

// SetClientIdPrefix set the prefix of the client id
func SetClientIdPrefix(prefix string) {
	clientIdPrefix = prefix
}
