use super::*;

fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |var| {
        pairs
            .iter()
            .find(|(name, _)| *name == var)
            .map(|(_, value)| (*value).to_owned())
    }
}

#[test]
fn credentials_need_the_key_id_and_the_secret() {
    credentials(env(&[(ACCESS_KEY_VAR, "id"), (SECRET_KEY_VAR, "s")])).unwrap();
    for (pairs, missing) in [
        (&[][..], ACCESS_KEY_VAR),
        (&[(SECRET_KEY_VAR, "s")][..], ACCESS_KEY_VAR),
        (&[(ACCESS_KEY_VAR, "id")][..], SECRET_KEY_VAR),
        (
            &[(ACCESS_KEY_VAR, "id"), (SECRET_KEY_VAR, "")][..],
            SECRET_KEY_VAR,
        ),
    ] {
        let err = credentials(env(pairs)).unwrap_err();
        assert!(
            matches!(err, CliError::MissingCredentials { var } if var == missing),
            "{err:?}"
        );
    }
}

#[test]
fn a_session_token_is_optional_and_never_printed() {
    let with = credentials(env(&[
        (ACCESS_KEY_VAR, "id"),
        (SECRET_KEY_VAR, "s"),
        (SESSION_TOKEN_VAR, "CANARY-TOKEN"),
    ]))
    .unwrap();
    assert!(!format!("{with:?}").contains("CANARY"));
    let err = credentials(env(&[(SECRET_KEY_VAR, "CANARY-SECRET")])).unwrap_err();
    assert!(!err.to_string().contains("CANARY"));
}

#[test]
fn entropy_stays_below_its_bound_and_varies() {
    let entropy = OsEntropy::new();
    let draws: Vec<u64> = (0..64).map(|_| entropy.below(1000)).collect();
    assert!(draws.iter().all(|draw| *draw < 1000));
    assert!(draws.iter().any(|draw| *draw != draws[0]));
    assert_eq!(entropy.below(0), 0);
}
