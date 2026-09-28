# 04 · A breaker held by the broker

No breaker state in the process: open is a consumer that stopped consuming, and the timer that ends it is a message in a delay chain.

```mermaid
flowchart LR
  producer["Producer"] --> queue[("payments-provider.work")]
  subgraph c1["consumer 1"]
    b1{{"breaker\n(a consumer on/off,\na token in the chain)"}}
    l1[/"limit\n(learned from 429s)"/]
  end
  subgraph c2["consumer 2"]
    b2{{"breaker"}}
    l2[/"limit"/]
  end
  subgraph c3["consumer N"]
    b3{{"breaker"}}
    l3[/"limit"/]
  end
  queue --> c1
  queue --> c2
  queue --> c3
  b1 -. "wake token, after 2^k s" .-> chain[("rmq.delay.level.NN\n(17 queues, TTL 1s … 18h)")]
  chain -. "back to its own wake queue" .-> b1
  b1 --> l1
  l1 --> api[("Third-party API\n(flaky-upstream)")]
  b2 --> l2
  l2 --> api
  b3 --> l3
  l3 --> api
  permit[("probe-permit\n(1 token)")]
  dead[("work.dead")]
  parked[("work.parked")]
  rtrigger[["redrive-trigger\n(single active consumer)"]]
  b2 -. "half-open: take, call, return" .-> permit
  queue -. "delivery limit" .-> dead
  rtrigger -. "elects one replica" .-> c3
  c3 -. "redrive" .-> dead
  dead -. "back to work,\nor after 5 redrives" .-> parked
  queue -. "4xx or unreadable" .-> parked
  classDef new fill:#fde68a,stroke:#b45309,stroke-width:2px,color:#1c1917
  class b1,b2,b3,chain,permit,parked,rtrigger,l1,l2,l3 new
  linkStyle 4,5,6,7,8,9,10,11,12,14,15,16,17 stroke:#d97706,stroke-width:3px
```

<sub>Amber: new on that branch. From the README of `article/04-rabbitmq-only-breaker`.</sub>
