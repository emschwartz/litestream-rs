#!/usr/bin/env python3
"""The unaided judge of scripts/planted-bugs.py, over fake swarm answers: no build, no swarm.

    python3 scripts/test_planted_bugs.py
"""

import importlib.util
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("planted_bugs", Path(__file__).with_name("planted-bugs.py"))
pb = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pb)

CLEAN = "none 300\t"


def budgeted(n: int) -> dict:
    return {"name": "p", "package": "lsr-core", "unaided": n}


def miss() -> dict:
    return {"name": "p", "package": "lsr-core", "unaided_miss": "needs a Go build"}


def found(k: int, seed: str = "0xf4300") -> str:
    return f"{k} {seed}\tseed {seed} failed at step 9: a reason"


class Verdict(unittest.TestCase):
    def verdict(self, bug: dict, got: str, clean: str = CLEAN) -> str:
        return pb.unaided_verdict(bug, got, clean)[0]

    def test_found_within_budget_passes(self):
        self.assertEqual(self.verdict(budgeted(50), found(7)), "ok")

    def test_found_on_the_last_seed_of_the_budget_passes(self):
        self.assertEqual(self.verdict(budgeted(50), found(50)), "ok")

    def test_found_over_budget_fails(self):
        self.assertEqual(self.verdict(budgeted(50), found(51)), "OVER BUDGET")

    def test_never_found_fails(self):
        self.assertEqual(self.verdict(budgeted(50), "none 50\t"), "NOT FOUND")

    def test_a_miss_not_found_passes(self):
        self.assertEqual(self.verdict(miss(), "none 300\t"), "ok miss")

    def test_a_found_miss_is_stale_and_fails(self):
        self.assertEqual(self.verdict(miss(), found(120)), "STALE MISS")

    def test_the_clean_trees_own_failure_masks_a_find(self):
        self.assertEqual(self.verdict(budgeted(300), found(250, "0xf4400"), found(250, "0xf4400")), "MASKED")

    def test_a_plant_that_did_not_apply_or_build_fails(self):
        for got in ("STALE", "BUILD\nerror[E0432]", "ERROR\nno line", "timeout in seeds 1..2"):
            with self.subTest(got=got):
                self.assertFalse(self.verdict(budgeted(50), got).startswith("ok"))


class Fields(unittest.TestCase):
    def entry(self, **kw) -> dict:
        return {"name": "e", "package": "lsr-sim", **kw}

    def test_a_swarm_reachable_entry_with_neither_is_refused(self):
        self.assertEqual(len(pb.unaided_problems([self.entry()])), 1)

    def test_a_package_outside_the_swarm_needs_neither(self):
        self.assertEqual(pb.unaided_problems([self.entry(package="lsr-ltx")]), [])

    def test_either_field_alone_is_accepted(self):
        self.assertEqual(pb.unaided_problems([self.entry(unaided=50)]), [])
        self.assertEqual(pb.unaided_problems([self.entry(unaided_miss="needs a Go build")]), [])

    def test_both_fields_an_empty_reason_and_a_bad_budget_are_refused(self):
        for kw in ({"unaided": 5, "unaided_miss": "x"}, {"unaided_miss": " "}, {"unaided": 0},
                   {"unaided": "50"}, {"unaided": True}):
            with self.subTest(kw=kw):
                self.assertEqual(len(pb.unaided_problems([self.entry(**kw)])), 1)

    def test_entries_planting_one_patch_must_agree(self):
        a = self.entry(name="a", unaided=50)
        b = self.entry(name="b", patch="a", package="lsr-core", unaided_miss="x")
        self.assertEqual(len(pb.unaided_problems([a, b])), 1)

    def test_canaries_are_exempt(self):
        self.assertEqual(pb.unaided_problems([self.entry(canary="killed")]), [])


if __name__ == "__main__":
    unittest.main()
