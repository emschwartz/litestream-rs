# Mutation testing

`just mutants <crate>` runs cargo-mutants 27.1.0 over `lsr-<crate>` with nextest, four jobs, results in
`target/mutants/mutants.out/`. No exclude patterns are configured. Phase 2's exit criterion is that every
survivor in `ltx` and `core` is killed by a test or listed here with a reason. `core` is not yet run.

## ltx

Every mutant not listed below is caught or unviable (does not compile). Tests added for the first run's
survivors: `tests/ltx/pins.rs`, the `tests` modules of `checksum.rs`, `encoder.rs`, `frame.rs`, `ids.rs`
and `page.rs`.

Tally (581 generated, about 7 minutes at four jobs): first run 29 missed, 477 caught, 68 unviable, 7 timeouts;
final run 3 missed (all equivalent, below), 503 caught, 68 unviable, 7 timeouts (all below).

### Equivalent (no input can tell the mutant from the original)

| Where | Mutation | Why |
| --- | --- | --- |
| `checksum.rs` `Checksum::toggle` | `FLAG \| (a ^ b)` becomes `FLAG ^ (a ^ b)` | Both operands carry bit 63 (the type only holds flagged values), so `a ^ b` has it clear and OR and XOR of the flag agree. |
| `ids.rs` `Txid::from_str` | `(raw << 4) \| digit` becomes `^` | The shift leaves the low nibble zero and `digit` is below 16, so the operands never overlap. |
| `index.rs` `put_uvarint` | `(v & 0x7f) \| 0x80` becomes `^` | `v & 0x7f` has bit 7 clear, so OR and XOR of `0x80` agree. |

### Detected by non-termination (cargo-mutants reports TIMEOUT)

Each makes a loop spin forever, so any test that reaches it hangs and the run kills it. That is a failing
test, not a survivor, but it is reported separately and slower than a catch.

| Where | Mutation | Loop that spins |
| --- | --- | --- |
| `decoder.rs` `Decoder::feed` | body replaced by `Ok((0, None))` | every caller's feed loop makes no progress |
| `decoder.rs` `Decoder::feed_raw` | body replaced by `Ok((0, None))` | same |
| `decoder.rs` `feed_raw` `self.unit.len() < need` | `<` becomes `==` or `<=` | a full unit is never processed, so no bytes are consumed |
| `decoder.rs` `ReadDecoder::next_event` `self.pos == self.len` | `==` becomes `!=` | the buffer is never refilled, or is refilled forever |
| `decoder.rs` `read_retrying` | match guard becomes `true` | every read error is retried forever |
| `index.rs` `put_uvarint` `value >= 0x80` | `>=` becomes `<` | the shift loop never ends for a multi-byte value |
