# validation

Our model on two public datasets, through the same feature code the live job runs.
Each adapter prints what the file holds and the model's recall, precision and F1 on
the test rows. The model is one LightGBM fit with `ml/train.py`'s settings (not its
five-fit committee), with the cut-off where F1 is highest on the rows just before the
test. What a dataset does not have is switched off rather than faked
(`harness.capability_profile`): no call state, no region, no session timing, no
analysts' verdicts, so 22 of the 28 features remain. The memory of confirmed
accounts, with verdicts played out from IBM AML's labels, is in `thesis/results.md`.

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

## Results (PaySim 13 October, IBM AML 21 October 2026)

| dataset | what it holds | scored | recall | precision | F1 |
|---|---|---|---|---|---|
| PaySim | 6.36M simulated mobile-money transactions over 31 days, 8,213 fraud | its 532,909 TRANSFER rows, 4,097 fraud; 24 days train, 7 test (920 fraud) | 27.9% | 63.0% | 38.7% |
| IBM AML HI-Small | 5.08M transfers between bank accounts over 17 days, 5,177 laundering | the whole file, 60/20/20 by time; 1,797 laundering in test | 42.5% | 92.9% | 58.3% |

IBM's accounts include banks and companies, not only people, so it is for
information only.

## IBM AML against its paper

Scored as Altman et al. (NeurIPS 2023, arXiv:2306.16424v3, Table 2) score it: the
whole file, 60/20/20 by time, F1 on laundering, five runs; our cut-off is chosen on
the middle 20% (`thesis/raw/ibm_as_published.json`).

| model | F1 on HI-Small |
|---|---|
| GIN, published | 28.7 ± 1.1 |
| GIN + EU, published | 47.7 ± 7.9 |
| PNA, published | 56.8 ± 2.4 |
| this project | 59.3 ± 1.0 |
| GFP + LightGBM, published | 62.9 ± 0.3 |
| GFP + XGBoost, published | 63.2 ± 0.2 |

## At a 2% alert budget

An analyst checks the 2% riskiest transfers of the test part; how much of the fraud
is among them.

| dataset | alerts | fraud caught | alerts that are fraud |
|---|---|---|---|
| PaySim, all transactions (`--all-types`) | 3,229 | 48.4% (891 of 1,840) | 27.6% |
| the published PaySim model (CatBoost, `ris3abh/aml-p2p-fraud-detection`) | 3,246 | 49.4% (916 of 1,854) | 28.2% |
| IBM AML HI-Small | 20,313 | 83.5% (1,500 of 1,797) | 7.4% |
