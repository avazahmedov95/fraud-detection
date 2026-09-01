// Load the account population into Neo4j.
// Relationship edges between account holders were removed from the design: the
// kinship signal they carried (is_family) was an artefact of how the synthetic
// data was built, not a real one. The graph now serves the money-flow network
// written by the sink (mule fan-in/out, transfer rings).
// CSVs are mounted read-only at the Neo4j import dir from data-generator/out.
// Run with:  make load-graph   (after `make generate`)

// Constraint for fast MERGE / lookups.
CREATE CONSTRAINT person_pinfl IF NOT EXISTS
FOR (p:Person) REQUIRE p.pinfl IS UNIQUE;

// The account-age lookup runs by CARD under the default payee_identity mode,
// because that is the identity a sending bank holds for the payee. Without an
// index that lookup is a full label scan on every uncached transaction.
//
// An INDEX, not a constraint: card uniqueness happens to hold in the generated
// population (one card per person), but the whole point of the card-keyed mode
// is that a real person holds several cards - one of which may one day be
// loaded here. A uniqueness constraint would make that data unloadable.
CREATE INDEX person_card IF NOT EXISTS
FOR (p:Person) ON (p.card);

// Accounts.
LOAD CSV WITH HEADERS FROM 'file:///persons.csv' AS row
MERGE (p:Person {pinfl: row.pinfl})
  SET p.card             = row.card,
      p.network          = row.network,
      p.full_name        = row.full_name,
      p.bank_code        = row.bank_code,
      p.bank_name        = row.bank_name,
      p.region           = row.region,
      p.account_age_days = toInteger(row.account_age_days),
      p.is_fraud_account = (row.is_fraud_account = 'True');

// Sanity counts.
MATCH (p:Person) RETURN count(p) AS persons;
