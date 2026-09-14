# Runbooks

Ten pages for the twelve rules in `infra/monitoring/alerts.yml`. Each rule
exists because something in this repo went wrong in a way that looked fine from
outside, so each runbook starts from what was actually observed rather than from
the metric's name.

Two pairs share a page, because each pair is one fault seen twice: the SLO burn
rates differ only in window, and `FloorUnheld` is what
`FleetDisagreesWithTarget` looks like when the daemon that disagrees is the
elected one.

They assume the compose stack (`docker compose up -d`) or an equivalent
deployment; service names are the compose ones.

| Alert | Severity | Runbook |
| --- | --- | --- |
| ControlLoopStalled | critical | [ControlLoopStalled.md](ControlLoopStalled.md) |
| NoLeaderElected | critical | [NoLeaderElected.md](NoLeaderElected.md) |
| SplitBrain | critical | [SplitBrain.md](SplitBrain.md) |
| DeliveryContractBroken | critical | [DeliveryContractBroken.md](DeliveryContractBroken.md) |
| FleetDisagreesWithTarget | warning | [FleetDisagreesWithTarget.md](FleetDisagreesWithTarget.md) |
| FloorUnheld | warning | [FleetDisagreesWithTarget.md](FleetDisagreesWithTarget.md) |
| FleetShrunk | warning | [FleetShrunk.md](FleetShrunk.md) |
| EgressSheddingLocally | warning | [EgressSheddingLocally.md](EgressSheddingLocally.md) |
| DeadLetterQueueGrowing | warning | [DeadLetterQueueGrowing.md](DeadLetterQueueGrowing.md) |
| WorkQueueStalled | critical | [WorkQueueStalled.md](WorkQueueStalled.md) |
| DeliverySLOFastBurn | critical | [DeliverySLOBurn.md](DeliverySLOBurn.md) |
| DeliverySLOSlowBurn | warning | [DeliverySLOBurn.md](DeliverySLOBurn.md) |
