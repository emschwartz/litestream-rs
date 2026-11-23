//! The build stage over fields the walk did not type as the build reads them: a schema and
//! an accessor that disagree must be a refusal, never a key silently read as absent
//! (issue #197, item A10).

use super::build;
use crate::error::ProblemKind;
use crate::expand::ProcessEnv;
use crate::walk::{Field, Fields, Value, Walk};

fn top(name: &'static str, value: Value) -> Fields {
    Fields {
        fields: vec![Field {
            name,
            line: 3,
            value,
        }],
        ..Fields::default()
    }
}

#[test]
fn a_value_of_the_wrong_type_is_a_problem_not_an_absent_key() {
    for (name, value) in [("addr", Value::Bool(true)), ("exec", Value::Int(7))] {
        let mut walk = Walk::default();
        let config = build(&top(name, value.clone()), &ProcessEnv, &mut walk);
        assert!(
            walk.problems.iter().any(|p| matches!(
                &p.kind,
                ProblemKind::FieldType { key, .. } if *key == name
            )),
            "{name} = {value:?} gave {:?} and {config:?}",
            walk.problems
        );
    }
}

#[test]
fn null_is_absent_as_go_reads_it() {
    let mut walk = Walk::default();
    build(&top("addr", Value::Null), &ProcessEnv, &mut walk);
    assert!(walk.problems.is_empty(), "{:?}", walk.problems);
}
