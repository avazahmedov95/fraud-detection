# validation

This project's model on two public datasets, through the same feature extractor
the live job runs. Each adapter prints what the file holds and the model's recall,
precision and F1 on the held-out rows: one fit of `ml/train.py`'s recipe, its
cut-off at the F1 peak on the rows just before the test slice. What a dataset lacks
is switched off rather than faked (`harness.capability_profile`): no call state, no
region, no session timing, no analysts' verdicts - 18 of the 24 features remain.

| File | What it does |
|---|---|
| `harness.py` | the extractor over a foreign file, the profile, the cache, the scoring |
| `paysim_adapter.py` | PaySim |
| `ibm_aml_adapter.py` | IBM AML, HI-Small |
| `tests/` | `python -m pytest validation -q` |

The data files are downloaded by hand into this folder and are not in git. The
first run of each adapter builds a feature cache beside it (`*_features.npz`).

```bash
python paysim_adapter.py --file PS_20174392719_1491204439457_log.csv
python ibm_aml_adapter.py --file HI-Small_Trans.csv
```

## Results, 2026-10-03

| dataset | what it holds | scored | recall | precision | F1 |
|---|---|---|---|---|---|
| PaySim | 6.36M simulated mobile-money transactions over 31 days, 8,213 fraud | its 532,909 TRANSFER rows, 4,097 fraud; 24 days train, 7 test (920 fraud) | 27.6% | 63.8% | 38.5% |
| IBM AML HI-Small | 4.49M transfers between bank accounts over 17 days, 5,166 laundering | 60/20/20 by time; 1,653 laundering in test | 44.3% | 79.2% | 56.8% |

PaySim's clock is the hour, so the sub-hour features see nothing there. IBM's
accounts include banks and companies, so it reports for information only.
