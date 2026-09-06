# Security: what is missing, and what production would have to do

This is an inventory, not a plan. Nothing in it was fixed in the pass that
wrote it — that was the instruction, and the reason is sound: half-built auth
is worse than none, because it looks like a control. Every line reference below
was opened and read while writing this, not carried over from an earlier
summary.

The short version: **this stack has no authentication, no authorization, no
transport security, and no secret handling anywhere.** It is a demonstration of
a control-plane design, and it runs on a private Docker network on someone's
laptop. Every item below is a real gap if that changes.

---

## 1. No authentication or authorization on any HTTP surface

Everything the aggregator serves is open to anyone who can reach the port.

| Route | Where | What it exposes |
| --- | --- | --- |
| `POST /api/failure` | [Http.ts:288](../packages/aggregator/src/Http.ts#L288) | **Failure injection.** Sets an API's simulated failure rate. |
| `POST /subscriber/webhook` | [Http.ts:310](../packages/aggregator/src/Http.ts#L310) | Accepts events as the subscriber, feeding the delivery-integrity counters. |
| `GET /api/state` | [Http.ts:181](../packages/aggregator/src/Http.ts#L181) | Full fleet state, per replica. |
| `GET /api/stream` | [Http.ts:162](../packages/aggregator/src/Http.ts#L162) | The same, pushed continuously over SSE. |
| `GET /metrics` | `/metrics` | Everything, including per-API topology. |

The sharp one is the first. A route that makes a healthy API look broken is an
availability control, and it is reachable by anyone who can open a TCP
connection to the console. **Production**: it must not exist in a production
build — compile it out, or put it behind an explicit flag *and* authentication,
and treat the flag being on as a deployment error. Everything else here wants
an authenticating proxy in front, or auth middleware in
[Http.ts](../packages/aggregator/src/Http.ts), with `/metrics` scoped to the
monitoring network rather than the world.

Anyone who can reach `/subscriber/webhook` can also forge delivery-integrity
data, which is worth naming separately: it is the endpoint the README points at
to prove the sequence contract, so it is the endpoint an attacker would use to
make a broken stream look intact.

## 2. Administrative interfaces published to the host

| Service | Port | Where | Note |
| --- | --- | --- | --- |
| Envoy admin ×3 | 19001-19003 | [docker-compose.yml:79](../docker-compose.yml#L79), [envoy.yaml:19](../infra/envoy/envoy.yaml#L19) | Admin binds `0.0.0.0:9901`; carries `/quitquitquit`, a full config dump, and runtime modification. |
| RabbitMQ | 5672, 15672, 15692 | [docker-compose.yml:127](../docker-compose.yml#L127) | Management UI, `guest`/`guest`. |
| Prometheus | 9090 | [docker-compose.yml:330](../docker-compose.yml#L330) | Query API, unauthenticated. |
| Alertmanager | 9093 | [docker-compose.yml:342](../docker-compose.yml#L342) | Can silence alerts, unauthenticated. |
| Grafana | 3000 | [docker-compose.yml:371](../docker-compose.yml#L371) | Anonymous access on ([:359](../docker-compose.yml#L359)), Viewer role. |
| Aggregators | 8088, 8089 | [docker-compose.yml:192](../docker-compose.yml#L192), [:199](../docker-compose.yml#L199) | Section 1. |

`/quitquitquit` deserves being said out loud: it is an unauthenticated HTTP
endpoint that stops the proxy every request in this system goes through.

**Production**: none of these is published. Envoy's admin listener binds
loopback or a management interface only; RabbitMQ gets real users with per-vhost
permissions and the management plugin restricted; Prometheus, Alertmanager and
Grafana sit behind SSO.

## 3. No transport security on any hop

Every connection in this stack is plaintext:

- **HTTP** between subscriber, console and aggregator.
- **AMQP** to RabbitMQ ([docker-compose.yml:21](../docker-compose.yml#L21)) —
  no TLS, and `guest`/`guest` credentials in the clear.
- **Redis**, which carries the lease, the checkpoints and the outbox
  ([docker-compose.yml:181](../docker-compose.yml#L181)) — no `AUTH`, no TLS.
- **gRPC**, where Envoy pushes its stats:
  `ServerCredentials.createInsecure()` at
  [EnvoyPushSource.ts:166](../packages/aggregator/src/EnvoyPushSource.ts#L166).

**Production**: TLS everywhere, with the Redis and AMQP credentials coming from
a secret store rather than a compose file. The gRPC sink takes real credentials
in the same call that currently takes `createInsecure()`, and Envoy's
`grpc_service` gets a matching `transport_socket`.

## 4. Unauthenticated inputs that shape decisions

Two ports accept data that changes what this system believes:

- **The metrics sink** ([EnvoyPushSource.ts:164](../packages/aggregator/src/EnvoyPushSource.ts#L164))
  accepts a `StreamMetrics` call from anything that can reach it, and whatever
  it says becomes a `ReplicaReport` under the node id *it chose for itself*. A
  forged stream can invent replicas, report a healthy API as fully ejected, and
  drive the circuit to `OPEN` for everyone — the daemon fleet will stop working
  because it was told to.
- **The webhook receiver** (section 1) accepts anything shaped like a
  `CircuitEvent`.

This is the gap that matters most after failure injection, and it is new: the
polling ingestion path had the opposite property, because the aggregator chose
which admin URLs to trust. **Production**: mTLS on the metrics sink, with the
node id checked against the client certificate rather than taken from the
payload.

## 5. Published events are unsigned

The webhook sink sends `content-type`, `ce-partitionkey` and `idempotency-key`
([Events.ts:147-151](../packages/aggregator/src/Events.ts#L147-L151)) and
nothing that lets a subscriber verify origin. A subscriber that acts on these
events — stopping work, shedding load — cannot tell one from a forgery.

**Production**: an HMAC signature header over the body with a shared secret, or
mTLS to the subscriber. This is genuinely an added header rather than a
redesign; the partition key and idempotency key are already there.

## 6. No secret handling

There are no secrets in this repo today, and no mechanism for them either,
which is how the first one ends up committed. `.env` is tracked in git and
holds `HOST_WORKSPACE_FOLDER`; the moment it holds a Redis password, the
password is in the history.

**Production**: secrets come from the platform's secret store as mounted files
or injected env, `.env` is untracked, and CI scans history for credentials.

## 7. Data at rest

Redis holds the lease, the per-API checkpoints, and — since the durable outbox
— **the full body of every event that failed delivery**
([Outbox.ts](../packages/aggregator/src/Outbox.ts)). It has AOF on and a
volume, no password, and no encryption. RabbitMQ's quorum queues hold work
payloads on disk under the same conditions.

**Production**: whatever the payloads' classification demands — at minimum
authentication and TLS, and encryption at rest if the events describe anything
that matters. Worth noting that this repo's events carry API identifiers and
health counts, not user data; a fork that puts customer identifiers in
`CircuitEvent.data` changes this section entirely.

## 8. Operator tooling that breaks things on purpose

[`infra/chaos.mjs`](../infra/chaos.mjs) kills containers and injects upstream
failures. [`infra/flaky-upstream.mjs`](../infra/flaky-upstream.mjs) exposes
`/__fail`. Neither belongs anywhere near a production network, and neither is
gated by anything but not being deployed.

**Production**: these stay in the repo and out of the image. The image built by
the [Dockerfile](../Dockerfile) contains `packages/` only, which is the right
boundary — keep it that way.

## 9. Supply chain

What is done: base images are pinned by digest
([Dockerfile](../Dockerfile), [docker-compose.yml](../docker-compose.yml)), the
lockfile is committed and CI installs with `--frozen-lockfile`, and the runtime
image runs as the non-root `node` user.

What is not: no SBOM, no image or dependency scanning, no signature or
provenance attestation, and no policy on the AMQP client this repo depends on
despite it being an early-stage library with known silent bugs
([docs/decisions/001-amqp-client.md](decisions/001-amqp-client.md)).

## 10. HTTPS egress needs TLS interception

Already stated in the README's own limitations, and it is a security decision
rather than a networking one: any L7 signal — `consecutive_5xx`, the whole
basis of the circuit — requires Envoy to terminate TLS. Proxying via `CONNECT`
gives L4 only. That drives a certificate story, a private CA, and a decision
about what the proxy is allowed to see.

---

## What this list is not

It is not ordered by severity for a particular deployment, because that depends
on where the thing runs. If it were exposed to a network today, the order would
be: failure injection (1), the metrics sink (4), the admin ports (2), then
everything else.

And it is deliberately not a set of half-measures. A token check on
`/api/failure` while the metrics sink accepts anonymous input would move the
problem rather than solve it, and it would leave the next reader believing the
surface had been secured.
