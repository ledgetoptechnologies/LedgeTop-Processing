# Coordinator updater optimized-Python safety qualification

The historical installer and updater contain safety checks in both Python functions
and serialized Python scripts. Python `-O` removed their `assert` checks. The checks
now call a small local `_require` function which explicitly raises `ValueError` on
failure. The accepted conditions, allowlists, fixed historical profile, and update
activation behavior are unchanged. The helper is inside serialized `volume_census`
and inside each of the seven embedded scripts, so it survives source extraction.
No new schema or predecessor profile is accepted by this patch.

Validation on 2026-11-05:

```
python -m unittest tests.test_coordinator_optimized_safety -v
```

3/3 tests passed in 0.180 seconds on Windows. The tests inspect the actual scripts,
compile all seven actual helper definitions at optimization level 2, and execute
actual serialized census and backup negative cases in child `python -O` processes.
An invalid owner is rejected before census/copy, with the synthetic source unchanged
and backup empty. These are local synthetic guard tests, not installation, runtime
handover, physical backup recovery, or connected production qualification.
