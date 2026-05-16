package control

import (
	"fmt"
	"strings"

	"github.com/samber/oops"
)

var (
	ErrInvalidNamespace  = oops.Code("invalid_namespace").In("control.catalog").New("control: invalid namespace")
	ErrInvalidSpace      = oops.Code("invalid_space").In("control.catalog").New("control: invalid space")
	ErrInvalidEntity     = oops.Code("invalid_entity").In("control.catalog").New("control: invalid entity")
	ErrNamespaceNotFound = oops.Code("namespace_not_found").In("control.catalog").New("control: namespace not found")
	ErrSpaceNotFound     = oops.Code("space_not_found").In("control.catalog").New("control: space not found")
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

func normalizeEntityIdentity(namespace, space, entity string) (string, string, string, error) {
	namespace, space, err := normalizeSpaceIdentity(namespace, space)
	if err != nil {
		return "", "", "", err
	}
	entity = strings.TrimSpace(entity)
	if err := validateCatalogName(entity, ErrInvalidEntity); err != nil {
		return "", "", "", err
	}
	return namespace, space, entity, nil
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
