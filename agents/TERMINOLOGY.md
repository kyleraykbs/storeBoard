# Terminology

- **peer** → a named device. Declared in config `peers.<name>` with
  `endpoints` (`host:port`); this machine's own name is the top-level
  `peer` (hostname fallback while no groups exist).
- **group** → `groups.<name>`: a set of peers that share folders; every
  group must include this peer.
- **folder** → `groups.<g>.folders.<name>`: one shared directory. Names are
  global across groups, so a local mount override names just the folder.
- **mount point** → where a folder appears locally. Effective order: local
  `mounts.<name>` override → the group's `folders.<name>.mount` default →
  `<dataDir>/mounts/<name>`.
- **storage directory** → `dataDir`: the one directory holding all daemon
  state (store, journal, index, default mounts).
- **endpoint** → a declared `host:port` address other peers dial; bare
  hostnames are rejected until the protocol picks a default port.
- **send** → copy a local file into one of this machine's folders so it
  syncs onwards (the phone's share sheet, `storeboard send`). An ordinary
  local change, and never an overwrite: a name already taken is parked
  under the deterministic conflict name.
- **the library** (`mobile/`) → the engine bound into an app; the phone
  runs it as a full peer with no local socket, so the UI calls it in
  process. One engine, no client-only mode (DESIGN invariant 17).
