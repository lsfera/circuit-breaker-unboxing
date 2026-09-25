# 013 — The target as a fraction, not a count

**Status**: adopted 2026-09-12.

## Decision

The daemon policy maps the circuit to a fraction of the fleet (ramp
`[elected one, ¼, ½, all]`, `DEGRADED` ½, `OPEN` 0). Each daemon hashes its
instance id to `[0, 1)` and works when its position is below the fraction. No
index, no fleet size, and `--scale rmq-daemon=12` needs no restart.

## The cost, simulated

5,000 random fleets per row at fraction 0.5:

| fleet | 5th–95th active | ran zero |
|---|---|---|
| 3 | 0–3 | 13.6% |
| 5 | 1–4 | 3.2% |
| 20 | 6–14 | 0.0% |
| 100 | 42–58 | 0.0% |

Error shrinks as √n, so it is worst at the size this repo runs. Running zero
on `DEGRADED` is not imprecision; it is indistinguishable from `OPEN`.

## What makes it safe

- **A floor of one**: the daemon elected on the `floor` single-active-consumer
  queue always works while the fraction is non-zero. The floor queue is bound
  to `circuit.control`, so every published event re-elects it, and a 60 s lease
  stops a dead holder's claim outliving it. Zero-runs drop to 0%.
- **A hash that mixes.** FNV-1a put `daemon-0`…`daemon-5` in a straight line
  over 2% of the space — a systematic bias. SHA-256.
- **Monitoring compares daemons to each other**: `FleetDisagreesWithTarget` is
  `max(target_fraction) != min(target_fraction)` (a deaf daemon reports the
  last fraction it heard), and `FloorUnheld` watches the floor.

Growing the fleet from 5 to 10 at a fixed fraction changed no existing daemon's
state. The intended count is printed next to the actual one, because the gap is
this decision's price.
