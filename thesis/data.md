# Data

## Why we generate data

There is no public data of Uzbek card-to-card transfers. Banks keep it private, and
the market is small enough that removing names would not protect the people in it.
The public datasets that exist do not fit:

- the Kaggle credit card set is anonymised, so no decision can be explained;
- IBM Synthetic Data Sets are a paid product;
- several Kaggle UPI sets are themselves generated, without saying how.

So we generate the data ourselves, with every rule written down and the code
public (`data-generator/`). Every figure measured on it is a design target, not a
result from a real bank.

## How the generator works

**People.** 50,000 people in households of 1 to 6. A household shares a region,
and its members count as relatives. The 14 regions are weighted by population.
Every card number starts with a real bank's BIN, and banks are chosen by their
number of cards in circulation (Central Bank figures, 1 April 2026). A fifth of
people also receive money on a card at a second bank.

**Normal transfers.**
- Each person has 3 to 8 regular payees, about a third of them relatives; 5% of
  transfers go to someone new.
- Amounts centre on each person's own usual amount (about 133,000 UZS in the
  middle), and 90% are round sums, as people really send.
- A few people send very often, most send rarely.
- 18% of people travel between regions at road speed, so a transfer from another
  region is not by itself suspicious.
- 10% of normal transfers are confirmed during a phone call.

**Normal transfers that look like fraud**, so the task is not too easy:
- 8% go to a new payee or are a large one-off payment (rent, a car, a deposit);
- 1% of people collect money from many senders in a few hours (a wedding, a gift);
- some people split one large payment into several parts;
- 4% of people change their phone.

**Four fraud schemes**, 0.2% of all transfers:
- **APP:** one transfer from a victim talked into it. 45% of victims are on a call
  while confirming, and they confirm more slowly than usual. 60% of amounts are
  ordinary, 40% take most of the balance.
- **Account takeover:** 2 to 8 quick transfers. 60% come from the victim's own phone
  and region (malware); the rest come from a region the victim could not have
  reached in time.
- **Mule:** 3 to 10 people pay one account, 5 to 60 minutes apart; then the account
  sends the money on in one or two transfers. Half of the mules are ordinary
  people recruited for it.
- **Structuring:** 3 to 8 transfers, 10 to 90 minutes apart, each between 70% and
  99% of the 10,000,000 UZS reporting threshold.

A tenth of fraud is never reported: the behaviour is in the data, but the label
says "normal", as happens in a real bank.

## The dataset we use

Seed 42: 500,000 transfers in January 2025 (30 days). 916 of them are labelled
fraud (0.18%): APP 303, mule 249, account takeover 182, structuring 182. Real card
traffic is about 0.17% fraud, so the rate is realistic.

The data is split by time. The model learns on the first 80% and is tested on the
last 20%: the last six days, 100,000 transfers, 176 of them fraud.

## What the generator does not model

- No weekly or monthly rhythm (no paydays, no weekends), so the hour of day is a
  weak signal.
- Transfers between people only; no payments to shops.
- Fraud schemes do not adapt to the detector (`threats.md` covers how they could).
- Fraud episodes are independent; real mule networks share accounts and devices.
- No cross-border transfers.

## Public datasets

We run the same feature code on two public datasets (`validation/`). What a
dataset does not have is switched off, not faked. These datasets have no call
state, no region, no session timing and no analysts' decisions, so 18 of the 24
features remain.

- **PaySim:** 6.36 million simulated mobile-money transactions over 31 days. We use
  its 532,909 transfers between people, 4,097 of them fraud: 24 days to learn, 7 to
  test. Its clock counts hours, so features over minutes see nothing there, and it
  has no collection stage, so the mule rule finds nothing.
- **IBM AML (HI-Small):** 4.49 million transfers between bank accounts over 17 days,
  5,166 of them money laundering. Its accounts include banks and companies, not only
  people, so we report it for information only.

The results are in `results.md`.

Public data cannot test the payee side fully: the links between accounts that it
needs are exactly what a bank cannot publish.
