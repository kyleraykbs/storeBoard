# Notes

## storeboard engine notes (2026-10-07)

- `STOREBOARD_SCAN_INTERVAL` (duration) drives the scanner fast for tests
  and smoke; default 15 s.
- Test harness traps: allocate endpoint pairs with both listeners held
  (sequential `freeEndpoint` calls can hand out the same ephemeral port,
  which makes a daemon dial itself and reject the connection as
  "peer claims to be me"); `STOREBOARD_TEST_LOG=1` turns on engine+transport
  logging in tests.
- Version vectors: a local change must INHERIT the previous entry's vector
  and bump its own counter. Replacing it makes history look concurrent and
  turns every ordinary edit into a conflict.
