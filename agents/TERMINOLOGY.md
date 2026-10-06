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
