# validation

Our model on two public datasets, through the same feature code the live job runs.
Each adapter prints what the file holds and the model's recall, precision and F1 on
the test rows. The model is trained the way `ml/train.py` trains it, with the
cut-off where F1 is highest on the rows just before the test. What a dataset does
not have is switched off rather than faked (`harness.capability_profile`): no call
state, no region, no session timing, no analysts' verdicts, so 18 of the 24
features remain.

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
python paysim_adapter.py --file PS_20174392719_1491204439457_log.csv --all-types
python ibm_aml_adapter.py --file HI-Small_Trans.csv
```

Each also prints how much of the fraud is among the 2% riskiest test transfers (the
2% alert budget), the way the published PaySim model reports its result. That
reading needs no cut-off, so its model learns on all the rows before the test (on
PaySim all 24 training days), as that model did. `--all-types` scores every PaySim
transaction, as that model did, not only the transfers between people.

## Results, 2026-10-03

| dataset | what it holds | scored | recall | precision | F1 |
|---|---|---|---|---|---|
| PaySim | 6.36M simulated mobile-money transactions over 31 days, 8,213 fraud | its 532,909 TRANSFER rows, 4,097 fraud; 24 days train, 7 test (920 fraud) | 27.6% | 63.8% | 38.5% |
| IBM AML HI-Small | 4.49M transfers between bank accounts over 17 days, 5,166 laundering | 60/20/20 by time; 1,653 laundering in test | 44.3% | 79.2% | 56.8% |

PaySim's clock counts hours, so the features over minutes see nothing there. IBM's
accounts include banks and companies, not only people, so it is reported for
information only.

## At a 2% alert budget, 2026-10-07

An analyst checks the 2% riskiest transfers of the test part; how much of the fraud
is among them.

| dataset | alerts | fraud caught | alerts that are fraud |
|---|---|---|---|
| PaySim, all transactions (`--all-types`) | 3,229 | 48.4% (890 of 1,840) | 27.6% |
| the published PaySim model (CatBoost, `ris3abh/aml-p2p-fraud-detection`) | - | 49.4% (916 of 1,854) | - |
| IBM AML HI-Small | 17,949 | 81.6% (1,349 of 1,653) | 7.5% |

On PaySim our 18 features come close to the published model, which also uses
PaySim's own transaction type. On all PaySim transactions the F1-peak reading is
32.2% caught, 45.3% of alerts fraud, F1 37.7%.
