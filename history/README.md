# History

The record of how this system was arrived at, kept out of `docs/` on purpose.

`docs/` describes the system **as it is** — what it does, how to run it, how to
adopt it, and the decisions currently in force. Everything in here is a record
of a moment rather than a description of the present, and mixing the two makes
both worse: a reader looking for current behaviour has to work out which
paragraphs still apply, and a record loses its value the moment someone edits it
to stay true.

| | What it is |
| --- | --- |
| [journey.md](journey.md) | The design history: each fork, the options that were genuinely available, the one taken and what it cost. Written as it happened. |
| [findings.md](findings.md) | The long form of every finding running this thing produced, with the measurement behind each one. |
| [what-the-broker-taught.md](what-the-broker-taught.md) | What building the RabbitMQ control plane surfaced: what was verified and how, the client bugs it exposed, and the live runs. Split out of `docs/rmq-control-plane.md`, which now describes only what the system is. |
| [runs/](runs/) | One JSON record per instrumented run of the stack — what each container cost, inside what limits, while what was happening. Written by [`infra/instrument.mjs`](../infra/instrument.mjs). The baselines the documentation quotes are kept here; the rest are gitignored, because a run is only worth keeping if something cites it. |

Two things follow from these being history:

- **They are not edited to match the code.** Where they describe a mechanism
  that has since been replaced — the daemon fleet's index and fleet size, for
  instance, which [ADR 013](../docs/decisions/013-the-target-as-a-fraction.md)
  replaced with a published fraction — that is what the system did at the time,
  and the ADR is where the change is recorded.
- **They are not published to the site.** The site builds from `docs/`, so
  these are read in the repository, which is where a record belongs.

Decision records stay in `docs/decisions/` rather than moving here, because a
decision is in force until it is superseded. Where one has been, the original
text is kept with a dated amendment below it rather than rewritten — for the
same reason this directory exists.
