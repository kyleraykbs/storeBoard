package syncer

// addWant records a want as request would, without sending anything.
func (e *Engine) addWant(store string, p *pending) {
	e.mu.Lock()
	e.setWantLocked(store, p.entry.Path, p)
	e.mu.Unlock()
}

// isWanted reports whether any version of a path is wanted.
func (e *Engine) isWanted(store, path string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.wants[wantKey(store, path)]) > 0
}
