package control

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/samber/oops"
)

var ErrInvalidNode = oops.Code("invalid_node").In("control.node").New("control: invalid node")
var ErrNodeNotFound = oops.Code("node_not_found").In("control.node").New("control: node not found")

func validateNodeIdentity(nodeID, addr string) (string, string, error) {
	nodeID, err := normalizeNodeID(nodeID)
	if err != nil {
		return "", "", err
	}
	addr, err = normalizeNodeAddr(addr)
	if err != nil {
		return "", "", err
	}
	return nodeID, addr, nil
}

func normalizeNodeIdentity(nodeID string) (string, error) {
	return normalizeNodeID(nodeID)
}

func normalizeNodeID(nodeID string) (string, error) {
	nodeID = strings.TrimSpace(nodeID)
	switch {
	case nodeID == "":
		return "", fmt.Errorf("%w: node_id is required", ErrInvalidNode)
	case strings.ContainsRune(nodeID, '\x00'):
		return "", fmt.Errorf("%w: node_id contains NUL", ErrInvalidNode)
	default:
		return nodeID, nil
	}
}

func normalizeNodeAddr(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", fmt.Errorf("%w: addr is required", ErrInvalidNode)
	}
	if strings.Contains(addr, "://") || strings.ContainsRune(addr, '\x00') {
		return "", fmt.Errorf("%w: addr must be host:port", ErrInvalidNode)
	}
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("%w: addr must be host:port: %w", ErrInvalidNode, err)
	}
	if strings.TrimSpace(host) == "" {
		return "", fmt.Errorf("%w: addr host is required", ErrInvalidNode)
	}
	if err := validateNodePort(portText); err != nil {
		return "", err
	}
	return addr, nil
}

func validateNodePort(portText string) error {
	port, err := strconv.Atoi(portText)
	if err != nil {
		return fmt.Errorf("%w: addr port must be numeric: %w", ErrInvalidNode, err)
	}
	if port <= 0 || port > 65535 {
		return fmt.Errorf("%w: addr port must be 1-65535", ErrInvalidNode)
	}
	return nil
}
