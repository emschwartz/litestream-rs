# litestream-rs (working name)

An in-process Rust library that replicates a WAL-mode SQLite database to S3-compatible storage and restores from it, interchangeable with [Litestream](https://github.com/benbjohnson/litestream) 0.5.x on the same bucket. Not affiliated with or endorsed by the Litestream project.

Status: early. Nothing here is ready to protect data.

The contract is the replica, not Litestream's behaviour: this library reads whatever Go Litestream wrote, and Go `litestream restore` and the Go VFS read whatever this library writes.

- Plan, design goals and phases: `docs/PLAN.md`
- Rules for working in this repository: `AGENTS.md`
- Research notes the plan rests on: `docs/research/`
