//! Choosing the part size of one multipart upload from the object's length.
//!
//! S3 takes at most 10000 parts in one upload, so a part of the configured
//! size carries at most `10000 * part` bytes: 48.8 GiB at the default 5 MiB. An
//! object longer than that used to upload every part it could, be refused at
//! part 10001, wait, and send the same bytes again for ever. The staged file's
//! length is known before the upload starts ([`ObjectStore::begin_sized_upload`]),
//! so the part size is chosen then: the configured size, or the smallest one
//! that fits the object in 10000 parts when the configured size does not.
//! Go's uploader sizes parts from the configured size alone and has the same
//! ceiling, but Go does not retry the object for ever with it.
//!
//! The limits are S3's: 10000 parts, 5 GiB a part and 5 TiB an object. The last
//! is the one that can bind: at 5 TiB the part is about 524 MB, within 5 GiB. An
//! object over it is refused before a byte is sent, with
//! [`StoreError::ObjectTooLarge`].
//!
//! Memory: an upload that buffers its parts (one with no file behind it, `upload.rs`) holds
//! the parts it has in flight and one more. How many are in flight is [`held_width`]: the
//! `concurrency` setting, cut so that what is held fits [`HELD_BUDGET`]. At the default
//! 5 MiB part and a concurrency of 5 that is six parts, 30 MiB; at a 100 MiB part it is two
//! parts whatever the concurrency. A part larger than half the budget is still sent, one in
//! flight and one filling: the bound is then `2 * part`, which is 1 GiB for the 524 MB parts
//! of a 5 TiB object.
//!
//! [`ObjectStore::begin_sized_upload`]: lsr_core::ObjectStore::begin_sized_upload
//! [`StoreError::ObjectTooLarge`]: lsr_core::StoreError::ObjectTooLarge

use std::num::NonZeroUsize;

use crate::config::PartSize;

/// The bytes one upload may hold in parts that are buffers: the ones in flight and the one
/// more that fills or waits (`upload.rs`, `docs/decisions/D232.md`).
pub(crate) const HELD_BUDGET: u64 = 256 * 1024 * 1024;

/// How many parts of `part` bytes that hold their bytes one upload has in flight at once:
/// `concurrency`, or as many as leave room in [`HELD_BUDGET`] for one more part that fills
/// or waits, and never fewer than one. With one in flight the upload holds two parts, which
/// passes the budget only for a part larger than half of it.
pub(crate) fn held_width(concurrency: NonZeroUsize, part: u64) -> NonZeroUsize {
    let fit = HELD_BUDGET
        .checked_div(part)
        .unwrap_or(u64::MAX)
        .saturating_sub(1);
    let fit = NonZeroUsize::new(usize::try_from(fit).unwrap_or(usize::MAX));
    fit.map_or(NonZeroUsize::MIN, |fit| fit.min(concurrency))
}

/// The most parts S3 takes in one multipart upload.
pub(crate) const MAX_PARTS: u64 = 10_000;

/// The largest object S3 holds: 5 TiB.
pub(crate) const MAX_OBJECT: u64 = 5 * 1024 * 1024 * 1024 * 1024;

/// The part size to upload an object of `len` bytes with: `configured`, or
/// the smallest size that fits it in [`MAX_PARTS`] parts when `configured`
/// does not. `None` when no part size can carry it ([`MAX_OBJECT`]).
///
/// Never below `configured`, so an object that fit before is sent as before.
pub(crate) fn plan_part_size(configured: PartSize, len: u64) -> Option<PartSize> {
    if len > MAX_OBJECT {
        return None;
    }
    Some(configured.at_least(len.div_ceil(MAX_PARTS)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    /// S3's largest part.
    const MAX_PART: u64 = 5 * GIB;

    fn part(bytes: u64) -> PartSize {
        PartSize::new(bytes).unwrap()
    }

    fn parts(len: u64, part: PartSize) -> u64 {
        len.div_ceil(part.get())
    }

    fn width(concurrency: usize, part: u64) -> usize {
        held_width(NonZeroUsize::new(concurrency).unwrap(), part).get()
    }

    /// The setting stands while the parts in flight and one more fit the budget: the default
    /// five 5 MiB parts and ten, and the last part size at which each still fits.
    #[test]
    fn parts_that_fit_the_budget_keep_the_configured_concurrency() {
        assert_eq!(width(5, 5 * MIB), 5);
        assert_eq!(width(10, 5 * MIB), 10);
        assert_eq!(width(1, 100 * MIB), 1);
        // Six parts held: the largest part is a sixth of the budget.
        assert_eq!(width(5, HELD_BUDGET / 6), 5);
        assert_eq!(width(5, HELD_BUDGET / 6 + 1), 4);
    }

    /// A large part cuts how many are in flight to what the budget holds with one more
    /// filling, whatever the setting, and never below one.
    #[test]
    fn large_parts_are_cut_to_what_the_budget_holds() {
        assert_eq!(width(10, 100 * MIB), 1);
        assert_eq!(width(10, 64 * MIB), 3);
        assert_eq!(width(10, 32 * MIB), 7);
        assert_eq!(width(10, HELD_BUDGET / 2), 1);
        // Past half the budget one is still in flight: the bound is then two parts.
        assert_eq!(width(10, HELD_BUDGET), 1);
        assert_eq!(width(10, MAX_PART), 1);
        assert_eq!(width(1, u64::MAX), 1);
    }

    #[test]
    fn an_object_that_fits_keeps_the_configured_part() {
        for configured in [PartSize::MIN, part(100 * MIB)] {
            assert_eq!(
                plan_part_size(configured, MAX_PARTS * configured.get()),
                Some(configured)
            );
            assert_eq!(plan_part_size(configured, 1), Some(configured));
            assert_eq!(plan_part_size(configured, 0), Some(configured));
        }
    }

    #[test]
    fn one_byte_past_the_ceiling_grows_the_part_by_the_least() {
        let ceiling = MAX_PARTS * PartSize::MIN.get();
        let grown = plan_part_size(PartSize::MIN, ceiling + 1).unwrap();
        assert_eq!(grown.get(), PartSize::MIN.get() + 1);
        assert_eq!(parts(ceiling + 1, grown), MAX_PARTS);
    }

    #[test]
    fn the_trial_database_at_five_mib_parts_is_carried() {
        // 60 GiB of LZ4 output, over the 48.8 GiB a 5 MiB part reaches.
        let len = 60 * GIB;
        let chosen = plan_part_size(PartSize::MIN, len).unwrap();
        assert!(chosen > PartSize::MIN);
        assert!(parts(len, chosen) <= MAX_PARTS);
    }

    #[test]
    fn the_largest_object_is_carried_and_one_byte_more_is_refused() {
        let chosen = plan_part_size(PartSize::MIN, MAX_OBJECT).unwrap();
        assert!(chosen.get() <= MAX_PART);
        assert!(parts(MAX_OBJECT, chosen) <= MAX_PARTS);
        assert_eq!(plan_part_size(PartSize::MIN, MAX_OBJECT + 1), None);
        assert_eq!(plan_part_size(part(100 * MIB), MAX_OBJECT + 1), None);
    }

    proptest! {
        /// Never more in flight than the setting, never fewer than one, and what is held
        /// (the parts in flight and one more) fits the budget unless one in flight already
        /// does not; the cut is the least that fits.
        #[test]
        fn the_width_is_the_most_within_the_setting_that_the_budget_holds(
            concurrency in 1_usize..64,
            part in 1_u64..=6 * GIB,
        ) {
            let got = u64::try_from(width(concurrency, part)).unwrap();
            let most = u64::try_from(concurrency).unwrap();
            prop_assert!((1..=most).contains(&got));
            let held = |width: u64| width.saturating_add(1).saturating_mul(part);
            prop_assert!(held(got) <= HELD_BUDGET || got == 1);
            prop_assert!(got == most || held(got.saturating_add(1)) > HELD_BUDGET);
        }

        /// Every length up to S3's limit gets at most 10000 parts, at a legal part
        /// size, and never one below the configured size.
        #[test]
        fn every_object_up_to_the_limit_fits_in_ten_thousand_legal_parts(
            len in 0..=MAX_OBJECT,
            configured in 5 * MIB..=GIB,
        ) {
            let configured = part(configured);
            let chosen = plan_part_size(configured, len).expect("within the limit");
            prop_assert!(chosen >= configured);
            prop_assert!(chosen >= PartSize::MIN);
            prop_assert!(chosen.get() <= MAX_PART);
            prop_assert!(parts(len, chosen) <= MAX_PARTS);
        }

        /// The part grows only as far as it must: one byte less and the object would
        /// need more than 10000 parts (or the configured size already fits).
        #[test]
        fn the_part_grows_only_when_the_configured_one_does_not_fit(
            len in 0..=MAX_OBJECT,
            configured in 5 * MIB..=GIB,
        ) {
            let configured = part(configured);
            let chosen = plan_part_size(configured, len).expect("within the limit");
            if chosen > configured {
                prop_assert!(parts(len, part(chosen.get().saturating_sub(1))) > MAX_PARTS);
            } else {
                prop_assert!(parts(len, configured) <= MAX_PARTS);
            }
        }

        #[test]
        fn an_object_past_the_limit_is_refused(over in 1..=u64::MAX - MAX_OBJECT) {
            prop_assert_eq!(plan_part_size(PartSize::MIN, MAX_OBJECT.saturating_add(over)), None);
        }
    }
}
