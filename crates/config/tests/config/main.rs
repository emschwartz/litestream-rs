//! `lsr-config` against Go: Go's own config tests ported by scenario
//! (`go`), the four key classes and every out-of-scope feature (`classes`),
//! yaml.v2's readings measured with Go (`fidelity`), environment expansion
//! (`env`), upstream's example file (`upstream`), and the mapping onto
//! replicators (`map`).

#[cfg(test)]
mod classes;
#[cfg(test)]
mod common;
#[cfg(test)]
mod env;
#[cfg(test)]
mod fidelity;
#[cfg(test)]
mod go;
#[cfg(test)]
mod map;
#[cfg(test)]
mod upstream;
