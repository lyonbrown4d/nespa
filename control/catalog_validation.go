package control

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidNamespace  = errors.New("control: invalid namespace")
	ErrInvalidSpace      = errors.New("control: invalid space")
	ErrNamespaceNotFound = errors.New("control: namespace not found")
	ErrSpaceNotFound     = errors.New("control: space not found")
)

func normalizeNamespace(namespace string) (string, error) {
	namespace = strings.TrimSpace(namespace)
	if err := validateCatalogName(namespace, ErrInvalidNamespace); err != nil {
		return "", err
	}
	return namespace, nil
}

func normalizeSpaceIdentity(namespace, space string) (string, string, error) {
	namespace, err := normalizeNamespace(namespace)
	if err != nil {
		return "", "", err
	}
	space = strings.TrimSpace(space)
	if err := validateCatalogName(space, ErrInvalidSpace); err != nil {
		return "", "", err
	}
	return namespace, space, nil
}

func validateCatalogName(name string, kind error) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: name is required", kind)
	case len(name) > 128:
		return fmt.Errorf("%w: name length must be <= 128", kind)
	case strings.ContainsRune(name, '\x00'):
		return fmt.Errorf("%w: name contains NUL", kind)
	}
	for _, item := range name {
		if !catalogNameRune(item) {
			return fmt.Errorf("%w: name contains invalid character %q", kind, item)
		}
	}
	return nil
}

func catalogNameRune(item rune) bool {
	return item >= 'a' && item <= 'z' ||
		item >= 'A' && item <= 'Z' ||
		item >= '0' && item <= '9' ||
		item == '-' || item == '_' || item == '.'
}

func namespaceNotFound(namespace string) error {
	return fmt.Errorf("%w: %s", ErrNamespaceNotFound, namespace)
}

func spaceNotFound(namespace, space string) error {
	return fmt.Errorf("%w: %s/%s", ErrSpaceNotFound, namespace, space)
}
