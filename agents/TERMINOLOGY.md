# Terminology

- **peer** → a named device. Declared in config `peers.<name>` with
  `endpoints` (`host:port`); this machine's own name is the top-level
  `peer` (hostname fallback while no groups exist).
- **group** → `groups.<name>`: a set of peers that share stores; every
  group must include this peer. A store names a group; a child inherits its
  parent's when undeclared.
- **root** → the top-level `root`: one directory, the only place store
  content lives. A plain container, not a store. Default `~/Storeboard`.
- **store** → `stores.<id>`: one replicated directory. Its **canonical dir**
  is `<root>/<id>`, a real directory. Names are globally unique.
- **canonical dir** → `<root>/<id>`. Every store has one, flat under the
  root. No store's bytes are nested inside another's.
- **child store** → a store declared under another store's `stores`. It is
  presented at its `path` (default: its id) inside the parent, as a symlink
  to its canonical dir.
- **symlink** (config key `symlinks`) → an absolute path, declared per store,
  that becomes a symlink to the canonical dir. Never inherited. Replaces the
  v1 absolute `mounts`: a path is a name, never the storage.
- **mount type** (`sync`) → `local` (a replica that works offline) or
  `remote` (streamed from a holder). Undeclared inherits the parent's; the
  top-level default is local.
- **inheritance** → undeclared follows the parent: group, sync, permissions,
  backup opt-out. Declared means its own thing.
- **storage directory** → `dataDir`: the one directory holding daemon state
  (blobs, journals, history, trash). It lives inside the root by default —
  `<root>/.storeboard` — because that state is store data, not user data, and
  a root on a pool then keeps every byte of a store on that pool. Not
  user-facing; content does not live here.
- **endpoint** → a declared `host:port` address other peers dial; bare
  hostnames are rejected until the protocol picks a default port.
- **send** → copy a local file into one of this machine's stores so it syncs
  onwards (the phone's share sheet, `storeboard send`). An ordinary local
  change, and never an overwrite: a name already taken is parked under the
  deterministic conflict name.
- **takeover** → `storeboard store takeover <name> [--path PATH]`: move the
  files already sitting at a declared symlink path into the canonical dir and
  link the path. Never automatic; never overwrites.
- **the library** (`mobile/`) → the engine bound into an app; the phone runs
  it as a full peer with no local socket, so the UI calls it in process. One
  engine, no client-only mode (DESIGN invariant 17).