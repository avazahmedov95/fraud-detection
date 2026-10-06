# thesis

Notes for the thesis and for the reports to the supervisor. They describe the
system as it is now. Nothing in the running system reads them.

| File | What it is |
|---|---|
| `research.md` | the problem, the research question, what we found |
| `system.md` | how the system works, part by part |
| `data.md` | the data we generate, and the public datasets |
| `results.md` | quality and speed before and after the latest changes |
| `threats.md` | who attacks, what the system can see, what it cannot |
| `related-work.md` | the papers and sources we used |
| `raw/` | the numbers behind the twenty-dataset tables in `results.md` |

The longer notes these replace, with the history of every experiment, are in git:
`git show b2062f2:thesis/<file>`.

## Words used in these notes

- **Recall**: the share of fraud we catch. 70% means 70 of 100 frauds are held.
- **Precision**: the share of alerts that are real fraud.
- **F1**: one number that balances recall and precision.
- **PR-AUC**: a score from 0 to 1 for how well the model puts fraud above normal
  transfers, over all possible cut-offs. Higher is better.
- **Cut-off**: the risk score at which a transfer is held.
- **Hold** (REVIEW): the transfer waits until an analyst blocks or releases it.
- **Second look**: a second model checks transfers just under the cut-off.
- **p95 / p99**: 95 or 99 of every 100 decisions are faster than this.
- **APP fraud**: a fraudster talks the victim into sending money.
- **Account takeover (ATO)**: a fraudster uses the victim's account.
- **Mule**: an account that collects stolen money and passes it on.
- **Fan-in**: many senders paying one account in a short time.
- **Structuring**: splitting a large sum into transfers just under the reporting
  threshold.
