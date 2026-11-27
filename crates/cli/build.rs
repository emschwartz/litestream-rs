//! Names the commit the binary is built from, for `lsr version`. Reads the checkout with
//! `git rev-parse HEAD`, which works on a `git fetch --depth 1` of one commit. A build with no
//! git (a source tarball) gets no commit, and `LSR_VERSION` set in the environment wins over it
//! in the crate (`render::resolve_version`).

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=LSR_VERSION");
    for path in ["HEAD"].into_iter().chain(head_ref().as_deref()) {
        if let Some(path) = git(&["rev-parse", "--git-path", path]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Some(commit) = git(&["rev-parse", "HEAD"]).filter(|c| is_commit(c)) {
        println!("cargo:rustc-env=LSR_COMMIT={commit}");
    }
}

/// The ref `HEAD` points at, whose file moves when a commit is made on the branch.
fn head_ref() -> Option<String> {
    git(&["symbolic-ref", "--quiet", "HEAD"])
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8(out.stdout).ok()?.trim().to_owned())
}

fn is_commit(text: &str) -> bool {
    text.len() >= 12 && text.bytes().all(|b| b.is_ascii_hexdigit())
}
