package redis

import (
	"crypto"
	"encoding/hex"
)

func scriptSHA(script string) string {
	hasher := crypto.SHA1.New()
	if _, err := hasher.Write([]byte(script)); err != nil {
		return ""
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func (s *Server) storeScript(sha, script string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scripts[sha] = script
}

func (s *Server) scriptForSHA(sha string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	script, ok := s.scripts[sha]
	return script, ok
}

func (s *Server) flushScripts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scripts = make(map[string]string)
}
