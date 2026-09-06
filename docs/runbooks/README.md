# Runbooks

One per alert in `infra/monitoring/alerts.yml`. Each rule exists because
something in this repo went wrong in a way that looked fine from outside, so
each runbook starts from what was actually observed rather than from the
metric's name.

They assume the compose stack (`docker compose up -d`) or an equivalent
deployment; service names are the compose ones.

| Alert | Severity | Runbook |
| --- | --- | --- |
| ControlLoopStalled | critical | [ControlLoopStalled.md](ControlLoopStalled.md) |
| NoLeaderElected | critical | [NoLeaderElected.md](NoLeaderElected.md) |
| SplitBrain | critical | [SplitBrain.md](SplitBrain.md) |
| DeliveryContractBroken | critical | [DeliveryContractBroken.md](DeliveryContractBroken.md) |
| DeliverySLOFastBurn / SlowBurn | critical / warning | [DeliverySLOBurn.md](DeliverySLOBurn.md) |
| FleetDisagreesWithTarget | warning | [FleetDisagreesWithTarget.md](FleetDisagreesWithTarget.md) |
| DeadLetterQueueGrowing | warning | [DeadLetterQueueGrowing.md](DeadLetterQueueGrowing.md) |
