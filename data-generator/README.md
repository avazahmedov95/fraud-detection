# data-generator

Synthetic P2P card transfers calibrated to Uzbekistan (UzCard and HUMO, 14 regions,
the issuing banks of `banks.csv`), with four fraud patterns injected and labelled -
there is no public dataset of Uzbek transfers. The dataset of record: 500,000
transfers by 50,000 people over 30 days, 0.2% fraud, legitimate transfers that share
fraud's shapes, and a tenth of fraud never reported.

| File | What it does |
|---|---|
| `generator.py` | people, everyday transfers, fraud; writes `out/transactions.csv` and `out/persons.csv` |
| `config.py` | every parameter, in `GeneratorConfig` |
| `persons.py` | the population: cards, banks, regions, households, devices |
| `events.py` | one transfer row |
| `fraud_patterns.py` | APP (a coached victim), ATO (a taken-over account), MULE (fan-in to a drop account), STRUCTURING (sums just under the reporting threshold) |
| `travel.py` | journeys between regions, so a transfer from another region is not by itself fraud |
| `kafka_producer.py` | replays the CSV into `transactions.raw`, stamping `ingested_at` and the integrity hash |
| `integrity.py` | the ingress hash; byte-identical to sink-writer's copy |
| `payload_crypto.py` | AES-256-GCM payloads for `--encrypt`; byte-identical to stream-processor's copy |
| `tests/` | `python -m pytest data-generator -q` |

```bash
python generator.py --out ./out          # the dataset of record (seed 42)
python kafka_producer.py --file out/transactions.csv --realtime --speed 200 \
    --bootstrap localhost:29092 --topic transactions.raw
```

`localhost:29092` is Kafka's listener for the host; `kafka:9092` works only inside
the Docker network. The labels (`label_is_fraud`, `label_fraud_type`) stay in the
file: the producer never sends them.
