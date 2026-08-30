# Synthetic P2P Transaction Generator (Uzbekistan)

Foundation component for the real-time fraud-detection pipeline. Generates a
labelled, Uzbekistan-calibrated transaction stream that feeds Kafka → Flink (CEP)
→ ML, plus the account population for Neo4j.

> The dataset is a design fixture for prototype experimentation. Any detection
> metrics obtained from it are **design targets**, not measured findings, until
> validated against real integration data.

## Why synthetic

No public Uzbekistan-specific P2P dataset exists; UzCard/HUMO data is proprietary
and public portals expose only macro aggregates. A synthetic generator calibrated
to local parameters is the path to a reproducible, shareable benchmark — the
absence of such a dataset is itself a contribution opportunity.

## Calibration

| Parameter | Value | Source to confirm |
|---|---|---|
| Card BINs | UzCard `8600`, HUMO `9860` (valid Luhn, 16-digit) | UzCard/HUMO network specs |
| Identity | 14-digit PINFL-style id (synthetic) | MyID model |
| Amounts | log-normal, UZS | — |
| CB limits / control threshold | placeholders in `config.py` | Regulation No. 3759 |
| Geography | 14 regions, population-weighted | — |
| Fraud rate | 1.5% (configurable) | — |

All values live in `config.py` and are meant to be overridden.

## Output files

- **transactions.csv** — event stream, sorted by `event_time`
- **persons.csv** — accounts → `(:Person)` nodes for Neo4j

### Bank assignment

Cards are issued by bank in proportion to each bank's **cards in circulation**
(`cards_mln` in `banks.csv`; CBU figures as at 1 April 2026, as reported by
Kursiv Uzbekistan). Figures are published for 17 of the 35 banks; the remaining
~18.6 M cards are split evenly across the other 18, which is a documented
approximation, not a source figure.

This matters because it sets the **on-us rate** — the share of transfers where
both parties bank with the same institution, and therefore the share for which
the sending bank can know `receiver_account_age_days` at all. Uniform assignment
gives ~3%, which is a property of having 35 banks in a list rather than of the
market; share-weighted assignment gives ~7%.

Both figures assume sender and receiver are paired at random. Real transfer
partners are not random (colleagues share a payroll bank, families share a
branch), so the true on-us rate is higher than either. Treat ~7% as a lower
bound, not an estimate.

Set `WEIGHT_BANKS_BY_CARD_SHARE = False` in `config.py` for uniform assignment.

> Cards in circulation is a proxy for transfer volume, not a measurement of it.
> Xalq banki leads largely on state social payments (low-activity cards), while
> digital banks such as Uzum see far more transactions per card. No per-bank P2P
> volume statistics are published.

### Event schema

| Group | Fields | Notes |
|---|---|---|
| Raw (from switch) | `transaction_id, event_time, sender_*, receiver_*, amount_uzs, channel, device_id, *_region, sender_balance_before` | what Kafka ingests |
| Behavioural (session) | `active_call, secs_login_to_confirm` | signals the mobile channel observes during the session; `secs_login_to_confirm` is converted to a per-client z-score downstream |
| Enriched | `is_new_payee, receiver_account_age_days` | in production these come from the **Flink** stage via the Neo4j account lookup and Redis feature store — not from the raw message. Materialised here so a model can train directly. |
| Labels | `label_is_fraud, label_fraud_type` | ground truth; never available at inference |

## Travel

A share of people take real journeys (`travel.py`): they depart, spend hours in
transit at road speed, transact from the destination, and come back. Events
falling mid-journey are re-timed to just after arrival — placing one at the
origin and the next at the destination minutes later would manufacture an
impossible journey inside legitimate traffic.

This exists for the IMPOSSIBLE_TRAVEL rule, and the **legitimate** half is the
important one. A generator that injects only the fraud pattern guarantees the
rule detects it, which demonstrates nothing. What can be measured is whether the
rule leaves ~775 real inter-regional journeys alone while catching sessions that
continue from somewhere the account holder cannot be.

Distances come from the detector's own coordinate table
(`stream-processor/geo.py`) and the reachability ceiling from its config, so the
simulation cannot disagree with the detector about the geography. Consequently
the injected hijacks are detectable by construction — **their detection rate is
not a result**. The false-positive rate on legitimate travel is.

## Fraud patterns

| Type | Shape | Primary signals |
|---|---|---|
| **APP** | account-holder sends one unusually large amount to a new, fresh payee, often while on a call | new payee + amount spike + low receiver age + coached session |
| **ATO** | new device, then 2–4 rapid transfers out; 60% continue the session from a region the victim cannot have reached since they were last seen | device change, impossible travel, velocity |
| **STRUCTURING** | many transfers kept just under the control threshold, short window | sub-threshold clustering, velocity |
| **MULE** | fan-in from many senders, then fan-out to a few | graph fan-in/out, velocity |

The fraud mix is weighted toward **APP** (the central research target) by
*transaction* count, since each APP episode is a single transaction.

## Usage

```bash
pip install -r requirements.txt
python generator.py --persons 5000 --transactions 50000 --fraud-rate 0.015 --out ./out
```

Stream into Kafka for the Flink job (keyed by sender → ordered per-sender stream):

```bash
# dry run (no broker needed)
python kafka_producer.py --file out/transactions.csv --dry-run

# live, paced to original gaps, 200x compressed
python kafka_producer.py --file out/transactions.csv --realtime --speed 200 \
    --bootstrap localhost:9092 --topic transactions.raw
```

Load the graph into Neo4j (example Cypher):

```cypher
LOAD CSV WITH HEADERS FROM 'file:///persons.csv' AS r
MERGE (p:Person {pinfl: r.pinfl})
  SET p.card = r.card, p.network = r.network, p.region = r.region,
      p.account_age_days = toInteger(r.account_age_days);
```

Edges are not loaded here: the money-flow relationships are written by the
sink-writer from scored transactions, not generated up front.

## Files

```
config.py          calibration constants (edit these)
events.py          canonical event schema + builder
persons.py         synthetic population (PINFL, names, Luhn-valid cards)
travel.py          real journeys (negative control) + unreachable hijack origins
fraud_patterns.py  APP / ATO / STRUCTURING / MULE injection
generator.py       normal traffic + orchestration + CLI
kafka_producer.py  CSV → Kafka replay (batch or paced live stream)
```
