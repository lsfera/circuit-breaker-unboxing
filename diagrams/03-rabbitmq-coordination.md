# 03 · Breakers coordinated through the broker

The same in-process breakers, sharing a one-token probe permit, a redrive of the dead-letter queue and a fleet view.

```mermaid
flowchart LR
  producer["Producer"] --> queue[("payments-provider.work")]
  permit[("probe-permit\n(1 token)")]
  dead[("work.dead")]
  parked[("work.parked")]
  rtrigger[["redrive-trigger\n(SAC)"]]
  subgraph c1["consumer 1"]
    b1{{"breaker\n(cockatiel)"}}
    l1[/"limit\n(learned from 429s)"/]
  end
  subgraph c2["consumer 2"]
    b2{{"breaker\n(cockatiel)"}}
    l2[/"limit\n(learned from 429s)"/]
  end
  subgraph c3["consumer N"]
    b3{{"breaker\n(cockatiel)"}}
    l3[/"limit\n(learned from 429s)"/]
  end
  queue --> c1
  queue --> c2
  queue --> c3
  b1 <-.->|"half-open only"| permit
  b2 <-.->|"half-open only"| permit
  b3 <-.->|"half-open only"| permit
  b1 --> l1
  b2 --> l2
  b3 --> l3
  l1 --> api[("Third-party API\n(flaky-upstream)")]
  l2 --> api
  l3 --> api
  queue -.->|"exhausts delivery limit\n(real calls only)"| dead
  b1 -.->|"onReset"| rtrigger
  b2 -.->|"onReset"| rtrigger
  b3 -.->|"onReset"| rtrigger
  rtrigger -.->|"elects exactly one"| dead
  dead -->|"redrive pass"| queue
  dead -.->|"MAX_REDRIVES exceeded"| parked
  queue -.->|"4xx or unreadable"| parked
  b1 -.->|"breaker state"| prom[("Prometheus\n(fleet_open rule)")]
  b2 -.->|"breaker state"| prom
  b3 -.->|"breaker state"| prom
  classDef new fill:#fde68a,stroke:#b45309,stroke-width:2px,color:#1c1917
  class permit,dead,parked,rtrigger,prom,l1,l2,l3 new
  linkStyle 4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 stroke:#d97706,stroke-width:3px
```

<sub>Amber: new on that branch. From the README of `article/03-rabbitmq-coordination`.</sub>
